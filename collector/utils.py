import json
import logging
from threading import current_thread

import sys
import redis
import requests
from gunicorn.arbiter import Arbiter

from .env import SSL_VERIFY, REDIS_ENV, REDIS_PASSWORD, PROCESS_WINDOW

MAX_RETRIES = 3
LOGGER = logging.getLogger()

# pylama:ignore=E1101
requests.packages.urllib3.disable_warnings()

if not (REDIS_ENV and REDIS_PASSWORD):
    LOGGER.error('Environment not set properly for Redis')
    sys.exit(Arbiter.APP_LOAD_ERROR)

REDIS = redis.Redis(**json.loads(REDIS_ENV), password=REDIS_PASSWORD)


def ping_redis() -> bool:
    """Call ping on Redis."""
    try:
        return REDIS.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError):
        LOGGER.warning('Redis Ping unsuccessful')
        return False


def processed(key: str) -> bool:
    """If an account has been processed within the window."""
    return REDIS.get(key)


def set_processed(key: str) -> None:
    """Flag an account as processed with TTL."""
    REDIS.set(key, 1, ex=PROCESS_WINDOW)


class DataMissingError(Exception):
    """No data collected exception."""


class RetryFailedError(requests.HTTPError):
    """Exception raised when the HTTP retry fails.

    Behaves as requests.HTTPError.
    """


def retryable(method: str, *args, **kwargs) -> requests.Response:
    """Retryable HTTP request.

    Invoke a "method" on "requests.session" with retry logic.
    :param method: "get", "post" etc.
    :param *args: Args for requests (first should be an URL, etc.)
    :param **kwargs: Kwargs for requests
    :return: Response object
    :raises: HTTPError when all requests fail
    """
    thread = current_thread()
    request_kwargs = dict(verify=SSL_VERIFY)
    request_kwargs.update(kwargs)

    with requests.Session() as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = getattr(session, method)(*args, **request_kwargs)

                resp.raise_for_status()
            except (requests.HTTPError, requests.ConnectionError) as e:
                LOGGER.warning(
                    '%s: Request failed (attempt #%d), retrying: %s',
                    thread.name, attempt, str(e)
                )
                continue
            else:
                return resp

    raise RetryFailedError('All attempts failed')
