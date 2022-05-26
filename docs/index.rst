.. cognite-sdk documentation master file, created by
   sphinx-quickstart on Thu Jan 11 15:57:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cognite Python SDK Documentation
================================

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF). The package is tightly integrated with pandas, and helps you work easily and efficiently with data in Cognite Data Fusion (CDF). 

.. contents::
   :local:

Installation
^^^^^^^^^^^^
To install this package:

.. code-block:: bash

   pip install cognite-sdk

To upgrade the version of this package:

.. code-block:: bash

   pip install cognite-sdk --upgrade

To install this package without the pandas and NumPy support:

.. code-block:: bash

   pip install cognite-sdk-core

To install with pandas, geopandas and shapely support (equivalent to installing `cognite-sdk`).
However, this gives you the option to only have pandas (and NumPy) support without geopandas.

.. code-block:: bash

   pip install cognite-sdk-core[pandas, geo]


Contents
^^^^^^^^
.. cognite here is cognite.rst which has the tree and it is mapped

.. toctree::
   cognite
