import redis
import pytest
import requests

from collector import utils

# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.
# R0903 = Too few public methods

# pylint: disable=R0201,R0903


def test_success_ping_redis(mocker):
    """Ping check is done once only."""
    mock_redis = mocker.patch.object(utils, 'REDIS')
    utils.ping_redis()

    mock_redis.ping.assert_called_once()


def test_exception_ping_redis(mocker):
    """Unsuccessful ping to Redis should return False."""
    mock_redis = mocker.patch.object(utils, 'REDIS')
    mock_redis.ping.side_effect = redis.exceptions.ConnectionError()
    return_value = utils.ping_redis()

    mock_redis.ping.assert_called_once()
    assert return_value is False


def test_set_processed(mocker):
    """Redis set is called properly."""
    mock_redis = mocker.patch.object(utils, 'REDIS')

    utils.set_processed('x')

    mock_redis.set.assert_called_once_with('x', 1, ex=utils.PROCESS_WINDOW)


class TestRetryable:
    """Test suite for `utils.retryable`."""

    @pytest.fixture(autouse=True)
    def default_setup(self, mocker):
        """Set mock for session and response before every test run."""
        # pylama: ignore=W0201
        session_cls = mocker.patch.object(requests, 'Session')
        self.session = mocker.MagicMock()
        self.session.__enter__.return_value = self.session
        session_cls.return_value = self.session

        self.response = mocker.Mock()
        response_cls = mocker.patch.object(requests, 'Response')
        response_cls.return_value = self.response
        self.session.get.return_value = self.response
        self.session.post.return_value = self.response

    def test_response(self, mocker):
        """Test a response can be received."""
        resp = utils.retryable('get', 'http://some.thing')

        self.session.get.assert_called_once_with(
            'http://some.thing', verify=mocker.ANY
        )
        assert resp == self.response

    @pytest.mark.parametrize('method', ('get', 'GET', 'post', 'POST'))
    def test_method_selection(self, mocker, method):
        """Test method selection propagation."""
        utils.retryable(method, 'http://some.thing')

        getattr(self.session, method).assert_called_once_with(
            'http://some.thing', verify=mocker.ANY
        )

    def test_retry(self):
        """Should retry when first request fails."""
        self.response.raise_for_status.side_effect = \
            [requests.HTTPError(), None]

        utils.retryable('get', 'http://some.thing')

        assert self.session.get.call_count == 2

    def test_retry_failed(self):
        """Should retry as many as MAX_RETRIES."""
        self.response.raise_for_status.side_effect = requests.HTTPError()

        with pytest.raises(utils.RetryFailedError):
            utils.retryable('get', 'http://some.thing')

        assert self.session.get.call_count == utils.MAX_RETRIES
        assert utils.MAX_RETRIES > 1

    @pytest.mark.parametrize('ssl_verify', (True, False))
    def test_ssl_validate(self, monkeypatch, ssl_verify):
        """Should respect SSL_VERIFY settings."""
        monkeypatch.setattr(utils, 'SSL_VERIFY', ssl_verify)

        utils.retryable('get', 'http://some.thing')

        self.session.get.assert_called_once_with(
            'http://some.thing', verify=ssl_verify
        )
