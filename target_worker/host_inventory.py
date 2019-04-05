import base64
import json
import logging
import math

from threading import current_thread

import prometheus_metrics
from . import utils, HOST_INVENTORY_HOST, HOST_INVENTORY_PATH

LOGGER = logging.getLogger()
URL = f'{HOST_INVENTORY_HOST}/{HOST_INVENTORY_PATH}'


def _retrieve_hosts(headers: dict) -> dict:
    """Collect all hosts for account.

    Args:
        headers (dict): HTTP Headers that will be used to request data

    Returns:
        dict: Host collection
    """
    url = URL + '&page={}'

    # Perform initial request
    resp = utils.retryable(
        'get', url.format(1), headers=headers
    )
    resp = resp.json()
    results = resp['results']
    total = resp['total']
    # Iterate next pages if any
    pages = math.ceil(total / resp['per_page'])

    for page in range(2, pages):
        prometheus_metrics.METRICS['gets'].inc()
        resp = utils.retryable(
            'get', url.format(page), headers=headers
        )
        prometheus_metrics.METRICS['get_successes'].inc()
        results += resp.json()['results']

    return dict(results=results, total=total)


def worker(_source: str, source_id: str, dest: str, b64_identity: str):
    """Worker for host inventory.

    Args:
        _source (str): URL of the source
        source_id (str): Job identifier
        dest (str): URL where to pass data
        b64_identity (str): Red Hat Identity base64 string

    """
    thread = current_thread()
    LOGGER.debug('%s: Worker started', thread.name)

    identity = json.loads(base64.b64decode(b64_identity))
    account_id = identity.get('identity', {}).get('account_number')
    LOGGER.debug('to retrieve hosts of account_id: %s', account_id)

    # TODO: Check cached account list before proceed

    headers = {"x-rh-identity": b64_identity}

    out = _retrieve_hosts(headers)
    LOGGER.debug(
        'Received data for account_id=%s has total=%s',
        account_id, out.get('total')
    )

    # Build the POST data object
    data = {
        'account': account_id,
        'data': out,
    }

    # Pass to next service
    prometheus_metrics.METRICS['posts'].inc()
    try:
        utils.retryable('post', dest, json=data, headers=headers)
        prometheus_metrics.METRICS['post_successes'].inc()
    except utils.RetryFailedError as exception:
        LOGGER.error(
            '%s: Failed to pass data for "%s": %s',
            thread.name, source_id, exception
        )
        prometheus_metrics.METRICS['post_errors'].inc()

    LOGGER.debug('%s: Done, exiting', thread.name)