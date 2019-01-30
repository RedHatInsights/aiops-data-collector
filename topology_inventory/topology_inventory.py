# Invokes an internal API that returns data-points pertinent to what is
# formally referred to as "Topological Inventory". Concordant data-sets
# have actionable value in downstream analytics and use-cases.

import os
import json
import requests

import urllib3
from urllib3.connectionpool import InsecureRequestWarning


class TopologyInventoryClient:
    """'Topological Inventory' data-set Client.

    A client that invokes an internal API and returns data related to the
    'Topological Inventory' data-set.

    """

    def __init__(self, username, password, endpoint):
        """Initialize key values."""
        # glue together into a single end-point
        endpoint_with_credentials = "https://{}:{}@{}".format(
            username,
            password,
            endpoint
        )

        self.endpoint_with_credentials = endpoint_with_credentials

    def query_path(self, *paths):
        """Query the Topological Inventory API.

        Queries the Topological Inventory API given a user-defined path. The
        path is defined by that in the ManageIQ swagger.yaml documentation:
        https://github.com/ManageIQ/topological_inventory-api/blob/master/public/doc/swagger-2-v0.0.1.yaml

        Args:
            paths (list): REST path(s) wishing to be queries.

        Examples
        --------
            client = TopologyInventoryClient()

            # get all `containers` elements
            client.query_path('containers')

            # get all `container_group` elements
            client.query_path('container_groups')

            # get where `container_nodes` := 2, and return as DataFrame
            client.query_path('container_nodes', '2')

        Returns
        -------
            dict

        """
        try:
            # the API executes an insecure request, so disable such warnings
            urllib3.disable_warnings(InsecureRequestWarning)

            # fetch a data-set given the REST endpoint
            endpoint = os.path.join(self.endpoint_with_credentials, *paths)
            response = requests.get(endpoint, verify=False)

            response.raise_for_status()

            # get REST response
            out = response.json()

            # if response is not a sequence, i.e. scalar or hash, cast as list
            if not isinstance(out, list):
                out = [out]

            return out

        # raised if the end-point is invalid or not JSON compatible
        except json.decoder.JSONDecodeError:
            return None
