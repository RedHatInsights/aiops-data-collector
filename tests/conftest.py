from imp import reload
import os

import pytest

import collector


@pytest.fixture(autouse=True)
def patch_env(monkeypatch):
    """Set up environment."""
    # Set Test variables
    setenv = dict(
        TOPOLOGICAL_INVENTORY_HOST='http://topological:8080',
        TOPOLOGICAL_INVENTORY_PATH='api/topo/vX',
        HOST_INVENTORY_HOST='http://inventory:8080',
        HOST_INVENTORY_PATH='api/inventory/vX',
        NEXT_SERVICE_URL='http://nextservice:8080'
    )
    for item, value in setenv.items():
        monkeypatch.setenv(item, value)

    # Drop extra variables
    delenv = ('SSL_VERIFY',)
    for item in delenv:
        if item in os.environ:
            monkeypatch.delenv('SSL_VERIFY')

    # Reload modules
    reload(collector.env)
    reload(collector.host_inventory)
    reload(collector.topological_inventory)
