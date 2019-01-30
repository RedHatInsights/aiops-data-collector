from contextlib import suppress
import logging
import os
from threading import Thread, current_thread
from tempfile import NamedTemporaryFile
from uuid import uuid4

import requests
import prometheus_metrics

import custom_parser

logger = logging.getLogger()
CHUNK = 10240
MAX_RETRIES = 3


def _retryable(method: str, *args, **kwargs) -> requests.Response:
    """Retryable HTTP request.

    Invoke a "method" on "requests.session" with retry logic.
    :param method: "get", "post" etc.
    :param *args: Args for requests (first should be an URL, etc.)
    :param **kwargs: Kwargs for requests
    :return: Response object
    :raises: HTTPError when all requests fail
    """
    thread = current_thread()

    with requests.Session() as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = getattr(session, method)(*args, **kwargs)

                resp.raise_for_status()
            except (requests.HTTPError, requests.ConnectionError) as e:
                logger.warning(
                    '%s: Request failed (attempt #%d), retrying: %s',
                    thread.name, attempt, str(e)
                )
                continue
            else:
                return resp

    raise requests.HTTPError('All attempts failed')


def download_job(source_url: str, source_id: str, dest_url: str) -> None:
    """Spawn a thread worker for data downloading task.

    Requests the data to be downloaded and pass it to the next service
    :param source_url: Data source location
    :param source_id: Data identifier
    :param dest_url: Location where the collected data should be received
    """
    # When source_id is missing, create our own
    source_id = source_id or str(uuid4())

    def worker() -> None:
        """Download, extract data and forward the content."""
        thread = current_thread()
        logger.debug('%s: Worker started', thread.name)

        # Fetch data
        prometheus_metrics.METRICS['gets'].inc()
        try:
            resp = _retryable('get', source_url, stream=True)
        except requests.HTTPError as exception:
            logger.error(
                '%s: Unable to fetch source data for "%s": %s',
                thread.name, source_id, exception
            )
            prometheus_metrics.METRICS['get_errors'].inc()
            return

        prometheus_metrics.METRICS['get_successes'].inc()

        try:
            with NamedTemporaryFile(delete=False) as tmp_file:
                file_name = tmp_file.name

                for chunk in filter(None, resp.iter_content(chunk_size=CHUNK)):
                    tmp_file.write(chunk)

        except IOError as exception:
            logger.error(
                '%s: Unable to create temp file for "%s": %s',
                thread.name, source_id, exception
            )
            return

        # Unpack data and stream it

        # Build the POST data object
        data = {
            'id': source_id,
            'data': custom_parser.parse(file_name),
        }

        # Pass to next service
        prometheus_metrics.METRICS['posts'].inc()
        try:
            resp = _retryable('post', f'http://{dest_url}', json=data)
            prometheus_metrics.METRICS['post_successes'].inc()
        except requests.HTTPError as exception:
            logger.error(
                '%s: Failed to pass data for "%s": %s',
                thread.name, source_id, exception
            )
            prometheus_metrics.METRICS['post_errors'].inc()

        # Cleanup
        with suppress(IOError):
            os.remove(file_name)

        logger.debug('%s: Done, exiting', thread.name)

    thread = Thread(target=worker)
    thread.start()


def topology_client_download(topology_client, entity: str) -> list:
    """Download a list for a Topological Inventory Entity."""
    return topology_client.query_path(entity)


def download_topological_inventory_data(
        topology_client,
        source_id: str,
        dest_url: str
) -> None:
    """Spawn a thread worker for data downloading Topological Inventory Data.

    Requests the data to be downloaded and pass it to the next service
    :param topology_client: Instance of TopologyInventoryClient
           to assist downloading data
    :param source_id: Data identifier
    :param dest_url: Location where the collected data should be received
    """
    # When source_id is missing, create our own
    source_id = source_id or str(uuid4())

    def worker() -> None:
        """Download, extract data and forward the content."""
        thread = current_thread()
        logger.debug('%s: Worker started', thread.name)

        # Fetch data
        entities = ["container_nodes",
                    "volume_attachments",
                    "volumes",
                    "volume_types"]

        topology_client_data = {}

        for entity in entities:
            prometheus_metrics.METRICS['gets'].inc()
            try:
                topology_client_data[entity] =\
                    topology_client_download(topology_client, entity)
                prometheus_metrics.METRICS['get_successes'].inc()
            except requests.HTTPError as exception:
                prometheus_metrics.METRICS['get_errors'].inc()
                logger.error(
                    '%s: Unable to fetch source data for "%s": %s',
                    thread.name, source_id, exception
                )
                return

        # Build the POST data object
        data = {
            'id': source_id,
            'data': topology_client_data
        }

        # Pass to next service
        prometheus_metrics.METRICS['posts'].inc()
        try:
            _retryable('post', f'http://{dest_url}', json=data)
            prometheus_metrics.METRICS['post_successes'].inc()
        except requests.HTTPError as exception:
            logger.error(
                '%s: Failed to pass data for "%s": %s',
                thread.name, source_id, exception
            )
            prometheus_metrics.METRICS['post_errors'].inc()

        logger.debug('%s: Done, exiting', thread.name)

    thread = Thread(target=worker)
    thread.start()
