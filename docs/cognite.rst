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

Instantiate a new client
------------------------
Use this code to instantiate a client and get your login status. CDF returns an object with
attributes that describe which project and service account your API key belongs to. The :code:`client_name`
is an user-defined string intended to give the client a unique identifier. You
can provide the :code:`client_name` through the :code:`COGNITE_CLIENT_NAME` environment variable or by passing it directly to the :code:`CogniteClient` constructor.
All examples in this documentation assume that :code:`COGNITE_CLIENT_NAME` has been set.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `CogniteClient`_ and the functionality it exposes below.

Discover time series
--------------------
For the next examples, you will need to supply ids for the time series that you want to retrieve. You can find some ids by listing the available time series. Limits for listing resources default to 25, so the following code will return the first 25 time series resources.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> ts_list = c.time_series.list(include_metadata=False)

Plot time series
----------------
There are several ways of plotting a time series you have fetched from the API. The easiest is to call
:code:`.plot()` on the returned :code:`TimeSeries` or :code:`TimeSeriesList` objects. By default, this plots the raw
data points for the last 24 hours. If there are no data points for the last 24 hours, :code:`plot` will throw an exception.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_time_series = c.time_series.retrieve(id=<time-series-id>)
    >>> my_time_series.plot()

You can also pass arguments to the :code:`.plot()` method to change the start, end, aggregates, and granularity of the
request.

.. code:: python

    >>> my_time_series.plot(start="365d-ago", end="now", aggregates=["average"], granularity="1d")

The :code:`Datapoints` and :code:`DatapointsList` objects that are returned when you fetch data points, also have :code:`.plot()`
methods you can use to plot the data.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_datapoints = c.datapoints.retrieve(
    ...                     id=[<time-series-ids>],
    ...                     start="10d-ago",
    ...                     end="now",
    ...                     aggregates=["max"],
    ...                     granularity="1h"
    ...                 )
    >>> my_datapoints.plot()

.. NOTE::
    To use the :code:`.plot()` functionality you need to install :code:`matplotlib`.

Create an asset hierarchy
-------------------------
CDF organizes digital information about the physical world. Assets are digital representations of physical objects or
groups of objects, and assets are organized into an asset hierarchy. For example, an asset can represent a water pump
which is part of a subsystem on an oil platform.

At the top of an asset hierarchy is a root asset (e.g., the oil platform). Each project can have multiple root assets.
All assets have a name and a parent asset. No assets with the same parent can have the same name.

To create a root asset (an asset without a parent), omit the parent ID when you post the asset to the API.
To make an asset a child of an existing asset, you must specify a parent ID.

.. code::

    >>> from cognite.client import CogniteClient
    >>> from cognite.client.data_classes import Asset
    >>> c = CogniteClient()
    >>> my_asset = Asset(name="my first asset", parent_id=123)
    >>> c.assets.create(my_asset)

To post an entire asset hierarchy, you can describe the relations within your asset hierarchy
using the :code:`external_id` and :code:`parent_external_id` attributes on the :code:`Asset` object. You can post
an arbitrary number of assets, and the SDK will split the request into multiple requests. To make sure that the
assets are posted in the correct order, you can use the .create_hierarchy() function, which takes care of the
sorting before splitting the request into smaller chunks. However, note that the .create_hierarchy() function requires the
external_id property to be set for all assets.

This example shows how to post a three levels deep asset hierarchy consisting of three assets.

.. code::

    >>> from cognite.client import CogniteClient
    >>> from cognite.client.data_classes import Asset
    >>> c = CogniteClient()
    >>> root = Asset(name="root", external_id="1")
    >>> child = Asset(name="child", external_id="2", parent_external_id="1")
    >>> descendant = Asset(name="descendant", external_id="3", parent_external_id="2")
    >>> c.assets.create_hierarchy([root, child, descendant])

Wrap the .create_hierarchy() call in a try-except to get information if posting the assets fails:

- Which assets were posted. (The request yielded a 201.)
- Which assets may have been posted. (The request yielded 5xx.)
- Which assets were not posted. (The request yielded 4xx, or was a descendant of another asset which may or may not have been posted.)

.. code::

    >>> from cognite.client.exceptions import CogniteAPIError
    >>> try:
    ...     c.assets.create_hierarchy([root, child, descendant])
    >>> except CogniteAPIError as e:
    ...     assets_posted = e.successful
    ...     assets_may_have_been_posted = e.unknown
    ...     assets_not_posted = e.failed

Retrieve all events related to an asset subtree
-----------------------------------------------
Assets are used to connect related data together, even if the data comes from different sources; Time series of data
points, events and files are all connected to one or more assets. A pump asset can be connected to a time series
measuring pressure within the pump, as well as events recording maintenance operations, and a file with a 3D diagram
of the pump.

To retrieve all events related to a given subtree of assets, we first fetch the subtree under a given asset using the
:code:`.subtree()` method. This returns an :code:`AssetList` object, which has a :code:`.events()` method. This method will
return events related to any asset in the :code:`AssetList`.

.. code::

    >>> from cognite.client import CogniteClient
    >>> from cognite.client.data_classes import Asset
    >>> c = CogniteClient()
    >>> subtree_root_asset="some-external-id"
    >>> subtree = c.assets.retrieve(external_id=subtree_root_asset).subtree()
    >>> related_events = subtree.events()

You can use the same pattern to retrieve all time series or files related to a set of assets.

.. code::

    >>> related_files = subtree.files()
    >>> related_time_series = subtree.time_series()

Settings
========
Client configuration
--------------------
You can pass configuration arguments directly to the :code:`CogniteClient` constructor, for example to configure the base url of your requests and additional headers. For a list of all configuration arguments, see the `CogniteClient`_ class definition.

Environment configuration
-------------------------
You can set default configurations with these environment variables:

.. code:: bash

    # Can be overrided by Client Configuration
    $ export COGNITE_API_KEY = <your-api-key>
    $ export COGNITE_PROJECT = <your-default-project>
    $ export COGNITE_BASE_URL = http://<host>:<port>
    $ export COGNITE_CLIENT_NAME = <user-defined-client-or-app-name>
    $ export COGNITE_MAX_WORKERS = <number-of-workers>
    $ export COGNITE_TIMEOUT = <num-of-seconds>
    $ export COGNITE_FILE_TRANSFER_TIMEOUT = <num-of-seconds>

    # Global Configuration
    $ export COGNITE_DISABLE_PYPI_VERSION_CHECK = "0"
    $ export COGNITE_DISABLE_GZIP = "0"
    $ export COGNITE_DISABLE_SSL = "0"
    $ export COGNITE_MAX_RETRIES = <number-of-retries>
    $ export COGNITE_MAX_RETRY_BACKOFF = <number-of-seconds>
    $ export COGNITE_MAX_CONNECTION_POOL_SIZE = <number-of-connections-in-pool>
    $ export COGNITE_STATUS_FORCELIST = "429,502,503"

Concurrency and connection pooling
----------------------------------
This library does not expose API limits to the user. If your request exceeds API limits, the SDK splits your
request into chunks and performs the sub-requests in parallel. To control how many concurrent requests you send
to the API, you can either pass the :code:`max_workers` attribute when you instantiate the :code:`CogniteClient` or set the :code:`COGNITE_MAX_WORKERS` environment variable.

If you are working with multiple instances of :code:`CogniteClient`, all instances will share the same connection pool.
If you have several instances, you can increase the max connection pool size to reuse connections if you are performing a large amount of concurrent requests. You can increase the max connection pool size by setting the :code:`COGNITE_MAX_CONNECTION_POOL_SIZE` environment variable.

Extensions and core library
============================
Pandas integration
------------------
The SDK is tightly integrated with the `pandas <https://pandas.pydata.org/pandas-docs/stable/>`_ library.
You can use the :code:`.to_pandas()` method on pretty much any object and get a pandas data frame describing the data.

This is particularly useful when you are working with time series data and with tabular data from the Raw API.

Matplotlib integration
----------------------
You can use the :code:`.plot()` method on any time series or data points result that the SDK returns. The method takes keyword
arguments which are passed on to the underlying matplotlib plot function, allowing you to configure for example the
size and layout of your plots.

You need to install the matplotlib package manually:

.. code:: bash

    $ pip install matplotlib

:code:`cognite-sdk` vs. :code:`cognite-sdk-core`
------------------------------------------------
If your application doesn't require the functionality from the :code:`pandas`
or :code:`numpy` dependencies, you should install the :code:`cognite-sdk-core` library.

The two libraries are exactly the same, except that :code:`cognite-sdk-core` does not specify :code:`pandas`
or :code:`numpy` as dependencies. This means that :code:`cognite-sdk-core` only has a subset
of the features available through the :code:`cognite-sdk` package. If you attempt to use functionality
that :code:`cognite-sdk-core` does not support, a :code:`CogniteImportError` is raised.

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

Retrieve multiple assets by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve_multiple

Retrieve an asset subtree
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve_subtree

List assets
^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.list

Aggregate assets
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate

Search for assets
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.search

Create assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create

Create asset hierarchy
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create_hierarchy

Delete assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.delete

Update assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.update

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.assets
    :members:
    :show-inheritance:

Experimental features
=====================
.. WARNING::
    These features are subject to breaking changes and should not be used in production code.