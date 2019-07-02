import pytest
import yaml
import base64
import json
from unittest.mock import ANY

from collector import topological_inventory


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

    pass


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

        mock.assert_called_once_with(expected_service, 'x', headers=ANY)

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
            dict(host='topological', path=''), 'x', headers=ANY
        )

    def test_pass_headers(self, mocker):
        """Ensure  headers are passed properly."""
        entity = dict(main_collection='x', service='INVALID_SERVICE')
        headers = dict(header='value')
        mock = mocker.patch.object(topological_inventory, '_collect_data')

        topological_inventory._query_main_collection(entity, headers)

        mock.assert_called_once_with(ANY, ANY, headers={'header': 'value'})

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
            expected_service, ANY, ANY, ANY, headers=ANY
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
            dict(host='topological', path=''), ANY, ANY, ANY, headers=ANY
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
            ANY, ANY, ANY, ANY, headers={'header': 'value'}
        )

    def test_called_for_every_entry(self, mocker):
        """A subcollection should be collected for every main entry."""
        entity = dict(
            main_collection='x', sub_collection='y', foreign_key='fk'
        )
        data = dict(x=[
            {'id': 1, 'name': 'stub_object_1'},
            {'id': 2, 'name': 'stub_object_2'},
            {'id': 3, 'name': 'stub_object_3'},
        ])

        mock = mocker.patch.object(topological_inventory, '_collect_data')
        mock.side_effect = lambda _, _1, _2, item_id, **_kwargs: [item_id]

        output = topological_inventory._query_sub_collection(entity, data)

        assert mock.call_count == 3
        assert output == [1, 2, 3]

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

        mock.assert_called_once_with(
            ANY, 'x/1/y', ANY, ANY, headers=ANY
        )


class TestWorker:
    """Test suite for worker."""

    pass


class TestTopologicalInventoryData:
    """Test suite for topological_inventory_data."""

    pass


class TestTenantHeaderInfo:
    """Test suite for tenant_header_info."""

    def test_header_structure(self):
        """Test presence of important keys in output dict."""
        info = topological_inventory.tenant_header_info(None)

        assert 'acct_no' in info.keys()
        assert 'headers' in info.keys()
        assert 'x-rh-identity' in info['headers'].keys()

    def test_acct_no(self):
        """Test passing of account number."""
        info = topological_inventory.tenant_header_info(10)

        assert info['acct_no'] == 10

    def test_base64_identity_value(self):
        """Test if base64 encoded string matches for the value."""
        b64 = b'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiA0Mn19'
        info = topological_inventory.tenant_header_info(42)

        assert b64 == info['headers']['x-rh-identity']

    def test_base64_identity_structure(self):
        """Test if base64 encoded string contains expected properties."""
        info = topological_inventory.tenant_header_info(42)
        b64_identity = info['headers']['x-rh-identity']
        identity = json.loads(base64.b64decode(b64_identity))

        assert 'identity' in identity.keys()
        assert 'account_number' in identity['identity'].keys()
        assert identity['identity']['account_number'] == 42
