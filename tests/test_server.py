import collector
from server import APP

# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.

# pylint: disable=R0201


class TestRoot:
    """Test various use cases for the index route."""

    def test_route_with_no_worker(self, mocker):
        """Test index route when there is no worker set."""
        client = APP.test_client(mocker)
        url = '/'

        redis = mocker.MagicMock()
        mocker.patch.object(collector.utils, 'REDIS', redis)

        response = client.get(url)
        assert response.get_data() == \
            b'{"message":"No worker set","status":"Error","version":"1.0"}\n'
        assert response.status_code == 500

    def test_route_with_only_redis_present(self, mocker):
        """Test index route when only redis is present."""
        client = APP.test_client(mocker)

        worker = mocker.MagicMock()
        mocker.patch.object(collector, 'WORKER', worker)

        url = '/'

        redis = mocker.MagicMock()
        mocker.patch.object(collector.utils, 'REDIS', redis)

        response = client.get(url)
        assert response.get_data() == \
            b'{"message":"Required service not operational",' \
            b'"status":"Error","version":"1.0"}\n'
        assert response.status_code == 500

    def test_route_with_only_next_service_present(self, mocker):
        """Test index route when only next service is present."""
        client = APP.test_client(mocker)

        worker = mocker.MagicMock()
        mocker.patch.object(collector, 'WORKER', worker)

        url = '/'

        next_service_response = {'status_code': 200}
        mocker.patch('server.collector.utils.retryable',
                     side_effect=next_service_response)

        response = client.get(url)
        assert response.get_data() == \
            b'{"message":"Required service not operational",' \
            b'"status":"Error","version":"1.0"}\n'
        assert response.status_code == 500

    def test_route_with_both_required_services_present(self, mocker):
        """Test index route when both services are present."""
        client = APP.test_client(mocker)

        worker = mocker.MagicMock()
        mocker.patch.object(collector, 'WORKER', worker)

        url = '/'

        next_service_response = {'status_code': 200}
        mocker.patch('server.collector.utils.retryable',
                     side_effect=next_service_response)

        redis = mocker.MagicMock()
        mocker.patch.object(collector.utils, 'REDIS', redis)

        response = client.get(url)
        assert response.get_data() == \
            b'{"message":"Up and Running",' \
            b'"status":"OK","version":"1.0"}\n'
        assert response.status_code == 200
