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

Assets
------
Retrieve an asset by id
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve

^^^^^^^^^^^^^^^^

