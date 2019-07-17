import base64
import json
import yaml

import pytest

from collector import topological_inventory, utils


# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.
# R0903 = Too few public methods
# W0212 = Access to a protected member _load_yaml of a client class

# pylint: disable=R0201,R0903,W0212


class TestLoadYaml:
    """Test suite for _load_yaml."""

    def test_valid_file(self, mocker):
        """Valid and existent filename is passed to the function."""
        data = "- A\n- List\n- Of\n- Things"
        mock = mocker.patch('builtins.open', mocker.mock_open(read_data=data))

        output = topological_inventory._load_yaml('some.yaml')
        mock.assert_called_with('some.yaml')
        assert output == ['A', 'List', 'Of', 'Things']

    def test_file_not_found(self):
        """Should raise IOError when file is not found."""
        with pytest.raises(IOError):
            topological_inventory._load_yaml('some.yaml')

    def test_invalid_yaml(self, mocker):
        """Should not accept invalid YAML config."""
        data = "- Invalid\n-List"
        mocker.patch('builtins.open', mocker.mock_open(read_data=data))

        with pytest.raises(yaml.scanner.ScannerError):
            topological_inventory._load_yaml('some.yaml')


class TestUpdateFk:
    """Test suite for _update_fk."""

    def test_missing_fk_name(self):
        """Attribute fk_name is missing == no transformation."""
        data = [{"id": 1, "name": "X", "fk": 2}]
        data_t = topological_inventory._update_fk(data, None, 3)

        assert data == data_t

    def test_missing_fk_id(self):
        """No value for FK is provided == no transformation."""
        data = [{"id": 1, "name": "X", "fk": 2}]
        data_t = topological_inventory._update_fk(data, 'fk', None)

        assert data == data_t

    def test_transform_of_existing_key(self):
        """Should overwrite existing FK value."""
        data = [{"id": 1, "name": "X", "fk": 2}]
        data_t = topological_inventory._update_fk(data, 'fk', 3)

        assert data_t[0]['fk'] == 3

    def test_transform_of_new_key(self):
        """Should add a FK value if missing."""
        data = [{"id": 1, "name": "X"}]
        data_t = topological_inventory._update_fk(data, 'fk', 3)

        assert data_t[0]['fk'] == 3


class TestCollectData:
    """Test suite for _collect_data."""

    def test_get_single_page(self, mocker):
        """Collect a single page data."""
        retryable = mocker.patch.object(utils, 'retryable')
        page_0 = mocker.Mock()
        page_0.json.return_value = dict(data=[0, 1, 2], links={})
        retryable.return_value = page_0

        data = topological_inventory._collect_data(
            dict(host='host', path='path'), 'url'
        )

        assert data == [0, 1, 2]
        retryable.assert_called_once_with(
            'get', 'host/path/url', headers=None
        )

    def test_get_multiple_pages(self, mocker):
        """Collect paginated data."""
        retryable = mocker.patch.object(utils, 'retryable')
        page_0, page_1 = mocker.Mock(), mocker.Mock()
        page_0.json.return_value = {
            'data': [0, 1, 2],
            'links': dict(next='/next_page')
        }
        page_1.json.return_value = {
            'data': [3, 4, 5],
            'links': {}
        }
        retryable.side_effect = [page_0, page_1]

        data = topological_inventory._collect_data(
            dict(host='host', path='path'), 'url'
        )

        assert retryable.call_count == 2
        assert data == [0, 1, 2, 3, 4, 5]

        retryable.assert_any_call(
            'get', 'host/path/url', headers=None
        )
        retryable.assert_any_call(
            'get', 'host/next_page', headers=None
        )


class TestQueryMainCollection:
    """Test suite for _query_main_collection."""

    @pytest.mark.parametrize('entity,expected_service', [
        (dict(service='SOURCES'), 'sources'),
        (dict(service='TOPOLOGICAL'), 'topological')
    ])
    def test_service_selection(self, monkeypatch, mocker,
                               entity, expected_service):
        """Should select correct service endpoint."""
        monkeypatch.setattr(
            topological_inventory, 'SERVICES_URL',
            dict(TOPOLOGICAL='topological', SOURCES='sources')
        )
        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_main_collection(
            dict(main_collection='x', **entity)
        )

        mock.assert_called_once_with(
            expected_service, 'x', headers=mocker.ANY
        )

    def test_service_fallback(self, monkeypatch, mocker):
        """Ensure invalid service selection falls back to Topological."""
        entity = dict(main_collection='x', service='INVALID_SERVICE')
        monkeypatch.setattr(
            topological_inventory, 'TOPOLOGICAL_INVENTORY_HOST',
            'topological'
        )
        monkeypatch.setattr(
            topological_inventory, 'TOPOLOGICAL_INVENTORY_PATH', ''
        )

        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_main_collection(entity)

        mock.assert_called_once_with(
            dict(host='topological', path=''), 'x', headers=mocker.ANY
        )

    def test_pass_headers(self, mocker):
        """Ensure  headers are passed properly."""
        entity = dict(main_collection='x', service='INVALID_SERVICE')
        headers = dict(header='value')
        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_main_collection(entity, headers)

        mock.assert_called_once_with(
            mocker.ANY, mocker.ANY, headers={'header': 'value'}
        )

    def test_missing_main_collection(self):
        """Should raise KeyError when main_collection is not present."""
        entity = dict(service='TOPOLOGICAL')

        with pytest.raises(KeyError):
            topological_inventory._query_main_collection(entity)


class TestQuerySubCollection:
    """Test suite for _query_sub_collection."""

    @pytest.mark.parametrize('entity,expected_service', [
        (dict(service='SOURCES'), 'sources'),
        (dict(service='TOPOLOGICAL'), 'topological')
    ])
    def test_service_selection(self, monkeypatch, mocker,
                               entity, expected_service):
        """Should select correct service endpoint."""
        monkeypatch.setattr(
            topological_inventory, 'SERVICES_URL',
            dict(TOPOLOGICAL='topological', SOURCES='sources')
        )
        mock = mocker.patch.object(topological_inventory, '_collect_data')

        data = dict(x=[{'id': 1, 'name': 'stub_object'}])
        entity.update(
            dict(main_collection='x', sub_collection='y', foreign_key='fk')
        )

        topological_inventory._query_sub_collection(entity, data)

        mock.assert_called_once_with(
            expected_service, mocker.ANY, headers=mocker.ANY
        )

    def test_service_fallback(self, monkeypatch, mocker):
        """Ensure invalid service selection falls back to Topological."""
        entity = dict(
            main_collection='x', sub_collection='y', foreign_key='fk',
            service='INVALID_SERVICE'
        )
        data = dict(x=[{'id': 1, 'name': 'stub_object'}])

        monkeypatch.setattr(
            topological_inventory, 'TOPOLOGICAL_INVENTORY_HOST',
            'topological'
        )
        monkeypatch.setattr(
            topological_inventory, 'TOPOLOGICAL_INVENTORY_PATH', ''
        )

        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_sub_collection(entity, data)

        mock.assert_called_once_with(
            dict(host='topological', path=''), mocker.ANY, headers=mocker.ANY
        )

    def test_pass_headers(self, mocker):
        """Ensure  headers are passed properly."""
        entity = dict(
            main_collection='x', sub_collection='y', foreign_key='fk',
            service='INVALID_SERVICE'
        )
        data = dict(x=[{'id': 1, 'name': 'stub_object'}])
        headers = dict(header='value')

        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_sub_collection(entity, data, headers)

        mock.assert_called_once_with(
            mocker.ANY, mocker.ANY, headers={'header': 'value'}
        )

    def test_called_for_every_entry(self, mocker):
        """A subcollection should be collected for every main entry."""
        entity = dict(
            main_collection='x', sub_collection='y', foreign_key='fk_x'
        )
        data = dict(x=[{'id': i, 'name': 'main_1'} for i in range(1, 4)])

        mock = mocker.patch.object(topological_inventory, '_collect_data')
        mock.side_effect = [
            [{'id': i, 'name': f'sub_{i}'}] for i in range(4, 7)
        ]

        output = topological_inventory._query_sub_collection(entity, data)

        assert mock.call_count == 3
        print(output)
        assert output == [
            {'id': i, 'name': f'sub_{i}', 'fk_x': i-3} for i in range(4, 7)
        ]

    @pytest.mark.parametrize('entity', [
        dict(sub_collection='y', foreign_key='fk'),
        dict(main_collection='x', foreign_key='fk'),
        dict(main_collection='x', sub_collection='y'),
    ])
    def test_missing_entity_part(self, entity):
        """Should raise KeyError when something is missing."""
        with pytest.raises(KeyError):
            topological_inventory._query_sub_collection(entity, {})

    def test_url_format(self, mocker):
        """Subcollection URL should be formed properly."""
        entity = dict(
            main_collection='x', sub_collection='y', foreign_key='fk'
        )
        data = dict(x=[
            {'id': 1, 'name': 'stub_object_1'},
        ])

        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_sub_collection(entity, data)

        mock.assert_called_once_with(mocker.ANY, 'x/1/y', headers=mocker.ANY)


class TestWorker:
    """Test suite for worker."""

    def test_all_tenants(self, monkeypatch, mocker):
        """All tenants should be collected if the switch is set."""
        monkeypatch.setattr(topological_inventory, 'ALL_TENANTS', True)
        account = dict(
            account_id=1,
            b64_identity=b'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAxfX0='
        )

        mock_collector = mocker.patch.object(
            topological_inventory, 'topological_inventory_data',
            return_value=0
        )
        mock_redis = mocker.patch.object(utils, 'set_processed')
        mock_collect_data = mocker.patch.object(
            topological_inventory, '_collect_data',
            return_value=[
                dict(external_tenant=i) for i in range(10)
            ]
        )

        topological_inventory.worker('', 'source_id', 'dest', account)

        assert mock_collector.call_count == 10
        mock_redis.assert_has_calls([mocker.call(i) for i in range(10)])
        mock_collect_data.assert_called_once_with(
            topological_inventory.SERVICES_URL['TOPOLOGICAL_INTERNAL'],
            'tenants', headers=mocker.ANY
        )

    def test_single_account(self, monkeypatch, mocker):
        """Single tenant is collected based on `acct_info`."""
        monkeypatch.setattr(topological_inventory, 'ALL_TENANTS', False)
        account = dict(
            account_id=1,
            b64_identity=b'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAxfX0='
        )

        mock_collector = mocker.patch.object(
            topological_inventory, 'topological_inventory_data',
            return_value=0
        )
        mock_redis = mocker.patch.object(utils, 'set_processed')

        topological_inventory.worker('', 'source_id', 'dest', account)

        mock_collector.assert_called_once_with(
            '', 'source_id', 'dest',
            {'x-rh-identity': account['b64_identity']}, mocker.ANY
        )
        mock_redis.assert_called_once_with(1)


class TestTopologicalInventoryData:
    """Test suite for topological_inventory_data."""

    @pytest.fixture(autouse=True)
    def default_setup(self, mocker, monkeypatch):
        """Set mocker and QUERIES before every test run."""
        # pylama: ignore=W0201
        self.query_main = mocker.patch.object(
            topological_inventory, '_query_main_collection'
        )
        self.query_sub = mocker.patch.object(
            topological_inventory, '_query_sub_collection'
        )
        self.retryable = mocker.patch.object(utils, 'retryable')
        self.thread = mocker.Mock(name='thread')

        monkeypatch.setattr(
            topological_inventory, 'QUERIES', {
                'a': {
                    'main_collection': 'a'
                },
                'b': {
                    'main_collection': 'b'
                },
                'sub_of_a': {
                    'sub_collection': 'sub',
                    'main_collection': 'a',
                    'foreign_key': 'a_id'
                }
            }
        )

    def test_no_collections(self, monkeypatch):
        """Should not call next service if no queries are specified."""
        monkeypatch.setattr(topological_inventory, 'APP_CONFIG', [])

        topological_inventory.topological_inventory_data(
            None, 'stub_id', 'dest', {}, self.thread
        )

        self.retryable.assert_not_called()

    def test_invalid_collection(self, monkeypatch):
        """Should raise if collection is not present in QUERIES."""
        monkeypatch.setattr(topological_inventory, 'APP_CONFIG', ['c'])

        with pytest.raises(KeyError):
            topological_inventory.topological_inventory_data(
                None, 'stub_id', 'dest', {}, self.thread
            )

    def test_main_collections(self, monkeypatch):
        """Collect a main collection only."""
        monkeypatch.setattr(topological_inventory, 'APP_CONFIG', ['a'])
        self.query_main.return_value = [0, 1]

        topological_inventory.topological_inventory_data(
            None, 'stub_id', 'dest', {}, self.thread
        )

        self.query_main.assert_called_once_with(
            dict(main_collection='a'), headers={}
        )
        self.query_sub.assert_not_called()
        self.retryable.assert_called_once_with(
            'post', 'dest', headers={},
            json={
                'id': 'stub_id',
                'data': dict(a=[0, 1])
            }
        )

    def test_sub_collections(self, monkeypatch, mocker):
        """Collect a sub collection only."""
        monkeypatch.setattr(topological_inventory, 'APP_CONFIG', ['sub_of_a'])
        self.query_sub.return_value = [0, 1]

        topological_inventory.topological_inventory_data(
            None, 'stub_id', 'dest', {}, self.thread
        )

        self.query_main.assert_not_called()
        self.query_sub.assert_called_once_with(
            {
                'main_collection': 'a',
                'sub_collection': 'sub',
                'foreign_key': 'a_id'
            },
            # _query_sub_collection is called with mutable data['data']
            mocker.ANY,
            headers={}
        )
        self.retryable.assert_called_once_with(
            'post', 'dest', headers={},
            json={
                'id': 'stub_id',
                'data': dict(sub_of_a=[0, 1])
            }
        )

    @pytest.mark.parametrize('main,sub,calls', [
        ([[], ['b_item']], [['sub_of_a_item']], [1, 0]),
        ([['a_item'], []], [['sub_of_a_item']], [2, 0]),
        ([['a_item'], ['b_item']], [[]], [2, 1]),
    ])
    def test_no_data(self, monkeypatch, main, sub, calls):
        """Should return if any collection is empty."""
        monkeypatch.setattr(
            topological_inventory, 'APP_CONFIG',
            ['a', 'b', 'sub_of_a']
        )

        self.query_main.side_effect = main
        self.query_sub.side_effect = sub

        topological_inventory.topological_inventory_data(
            None, 'stub_id', 'dest', {}, self.thread
        )
        assert self.query_main.call_count == calls[0]
        assert self.query_sub.call_count == calls[1]
        self.retryable.assert_not_called()

    def test_failed_retry(self, monkeypatch):
        """Should not pass data if collection fails due to exception."""
        monkeypatch.setattr(topological_inventory, 'APP_CONFIG', ['a'])

        self.query_main.side_effect = utils.RetryFailedError()

        topological_inventory.topological_inventory_data(
            None, 'stub_id', 'dest', {}, self.thread
        )

        self.query_main.assert_called_once_with(
            dict(main_collection='a'), headers={}
        )
        self.query_sub.assert_not_called()
        self.retryable.assert_not_called()


class TestTenant:
    """Test suite for create_tenant."""

    def test_header_structure(self):
        """Test presence of important keys in output dict."""
        info = topological_inventory.create_tenant(None)

        assert 'account_number' in info._fields
        assert 'headers' in info._fields
        assert 'x-rh-identity' in info.headers.keys()

    def test_acct_no(self):
        """Test passing of account number."""
        info = topological_inventory.create_tenant(10)

        assert info.account_number == 10

    def test_base64_identity_value(self):
        """Test if base64 encoded string matches for the value."""
        b64 = b'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiA0Mn19'
        info = topological_inventory.create_tenant(42)

        assert b64 == info.headers['x-rh-identity']

    def test_base64_identity_structure(self):
        """Test if base64 encoded string contains expected properties."""
        info = topological_inventory.create_tenant(42)
        b64_identity = info.headers['x-rh-identity']
        identity = json.loads(base64.b64decode(b64_identity))

        assert 'identity' in identity.keys()
        assert 'account_number' in identity['identity'].keys()
        assert identity['identity']['account_number'] == 42
