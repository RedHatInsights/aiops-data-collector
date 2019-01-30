import logging
import os

from flask import Flask, jsonify, request
from flask.logging import default_handler

import workers
import prometheus_metrics

from collect_json_schema import CollectJSONSchema
from topology_json_schema import TopologyJSONSchema


def create_application():
    """Create Flask application instance with AWS client enabled."""
    app = Flask(__name__)
    app.config['NEXT_MICROSERVICE_HOST'] = \
        os.environ.get('NEXT_MICROSERVICE_HOST')
    app.config['DATA_COLLECTION_TYPE'] = \
        os.environ.get('DATA_COLLECTION_TYPE')

    return app


APP = create_application()
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(APP.logger.level)
ROOT_LOGGER.addHandler(default_handler)

VERSION = "0.0.1"

ROUTE_PREFIX = "/r/insights/platform/aiops-data-collector"

# Schema for the Collect API
SCHEMA = CollectJSONSchema()

# Schema for Topology
SCHEMA_TOPOLOGY = TopologyJSONSchema()


@APP.route(ROUTE_PREFIX, methods=['GET'])
def get_root():
    """Root Endpoint for 3scale."""
    return jsonify(
        status='OK',
        version=VERSION,
        message='Up and Running'
    )


@APP.route(f'{ROUTE_PREFIX}/api/v0/version', methods=['GET'])
def get_version():
    """Endpoint for getting the current version."""
    return jsonify(
        status='OK',
        version=VERSION,
        message='AIOPS Data Collector Version 0.0.1'
    )


@APP.route(f'{ROUTE_PREFIX}/api/v0/collect', methods=['POST'])
def post_collect():
    """Endpoint servicing data collection."""
    input_data = request.get_json(force=True)

    data_collection_type = APP.config['DATA_COLLECTION_TYPE']

    next_service = APP.config['NEXT_MICROSERVICE_HOST']
    source_id = input_data.get('payload_id')

    prometheus_metrics.METRICS['jobs_total'].inc()

    if data_collection_type.upper() == 'TOPOLOGY':
        import topology_inventory
        username = os.environ.get('USERNAME')
        password = os.environ.get('PASSWORD')
        endpoint = os.environ.get('TOPOLOGY_INVENTORY_ENDPOINT')

        validation = SCHEMA_TOPOLOGY.load(
            {'username': username,
             'password': password,
             'endpoint': endpoint
             }
        )
        if validation.errors:
            prometheus_metrics.METRICS['jobs_denied'].inc()
            return jsonify(
                status='Error',
                errors=validation.errors,
                message='Input payload validation failed for Topology'
            ), 400

        topology_client = topology_inventory.TopologyInventoryClient(
            username,
            password,
            endpoint
        )

        workers.download_topological_inventory_data(
            topology_client,
            source_id,
            next_service
        )
        APP.logger.info('Validation using Topological Inventory Job started.')
    else:
        validation = SCHEMA.load(input_data)
        if validation.errors:
            prometheus_metrics.METRICS['jobs_denied'].inc()
            return jsonify(
                status='Error',
                errors=validation.errors,
                message='Input payload validation failed'
            ), 400

        workers.download_job(input_data['url'], source_id, next_service)
        APP.logger.info('Job started.')

    prometheus_metrics.METRICS['jobs_initiated'].inc()
    return jsonify(status="OK", message="Job initiated")


@APP.route("/metrics", methods=['GET'])
def get_metrics():
    """Metrics Endpoint."""
    return prometheus_metrics.generate_aggregated_metrics()


if __name__ == "__main__":
    # pylama:ignore=C0103
    port = int(os.environ.get("PORT", 8004))
    APP.run(port=port)
