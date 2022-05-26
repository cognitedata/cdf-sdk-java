Quickstart
==========
Authenticate
------------

The preferred way to authenticating against the Cognite API is using OpenID Connect (OIDC). To enable this, the CogniteClient
accepts a token provider function.

.. code:: python

    >>> from cognite.client import CogniteClient

For details on different ways of implementing the token provider, take a look at
`this guide <https://github.com/cognitedata/python-oidc-authentication>`_.

Instantiate a new client
------------------------
All examples in this documentation assume that :code:`COGNITE_CLIENT_NAME` has been set.

.. code:: python

    >>> from cognite.client import CogniteClient

Read more about the `CogniteClient`_ and the functionality it exposes below.

API
===
CogniteClient
-------------
.. autoclass:: cognite.client.CogniteClient
    :members:
    :member-order: bysource

Authentication
--------------
Get login status
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.login.LoginAPI.status


Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.login
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Assets
------
Retrieve an asset by id
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve

^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate

Search for assets
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.search

Delete assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.delete

Update assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.update
