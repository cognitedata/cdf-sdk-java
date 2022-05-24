Quickstart
==========
Authenticate
------------

The preferred way to authenticating against the Cognite API is using OpenID Connect (OIDC). To enable this, the CogniteClient
accepts a token provider function.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> def token_provider():
    >>>     ...
    >>> c = CogniteClient(token=token_provider)