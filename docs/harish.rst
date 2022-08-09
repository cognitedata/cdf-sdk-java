RTD Here testing
========
How to?
--------

I am trying here to add a third menu and see how this is gettig displayed.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> def token_provider():
    >>>     ...
    >>> c = CogniteClient(token=token_provider)

For details on different ways of implementing the token provider, take a look at
`this guide <https://github.com/cognitedata/python-oidc-authentication>`_.

If OIDC has not been enabled for your CDF project, you will want to authenticate using an API key. You can do this by setting the following environment
variable

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>

or by passing the API key directly to the CogniteClient.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient(api_key="<your-api-key>", client_name="<your-client-name>")

How do I do it?
----------------
Some random text comes here. 

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `CogniteClient`_ and the functionality it exposes below.

Discover time series
--------------------
For the next examples, you will need to supply ids for the time series that you want to retrieve. You can find some ids by listing the available time series. Limits for listing resources default to 25, so the following code will return the first 25 time series resources.
