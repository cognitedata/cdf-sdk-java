[![java-main Actions Status](https://github.com/cognitedata/cdf-beam-connector-java/workflows/java-main/badge.svg)](https://github.com/cognitedata/cdf-beam-connector-java/actions)

<a href="https://cognite.com/">
    <img src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# Beam sdk for CDF


Beam I/O connector for reading and writing from/to CDF resources.

The connector implements a sub-connector per resource type (Asset, Event, etc.) and is configured
by passing it a config object and optionally a query object.

```java
PCollection<Asset> mainInput = p.apply("Read cdf assets", CogniteIO.readAssets()
                   .withConfig(ProjectConfig.create()
                           .withProject(<project>)
                           .withApiKey(<api key>)
                   )
                   .withRequestParameters(RequestParameters.create()
                       .withFilterParameter("name", "value")
                       )
           );
```
    
#### Installing the sdk

````xml
<dependency>    
    <groupId>com.cognite</groupId>
    <artifactId>beam-connector-java</artifactId>
    <version>0.9.15</version>
</dependency>
````
    
#### I/O feature table

| Resource | Read | Insert | Update | Delete | Read aggregates | Streaming read | Streaming write
| --- | --- | --- | --- | --- | --- | --- | --- |
| Time series header | Yes | Yes | Yes | Yes | Yes | Yes | Yes
| Time series data points | Yes | Yes | Yes | Yes, via time series delete. | Yes | Yes | Yes
| Assets | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Events | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Files, metadata | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Files, binary | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Sequences header | Yes | Yes | Yes | Yes | Yes | No | Yes |
| Sequences data points | Yes | Yes | Yes | Yes | No | No | Yes |
| Relationships | Yes | Yes,  via upsert | Yes, via upsert |Yes | No | No | Yes |
| 3D nodes | No | No | No | No | No | No | No |
| Raw database | Yes | Yes, via row insert | N/A | No | No | No | Yes |
| Raw table | Yes | Yes, via row insert | N/A | No | No | No | Yes |
| Raw table row | Yes | Yes | Yes | Yes | No | Yes | Yes |
| Data set | Yes | Yes | Yes | No | No | No | Yes |
| Labels | Yes | Yes | Yes | Yes | No | No | Yes |

#### Contextualization feature table
| Resource | Feature support |
| --- | --- |
| Entity matcher | Predict |
| Interactive P&ID | Detect annotations, convert |

### Accessing the I/O connectors

All connectors are accessed via static methods in ``com.cognite.beam.io.CogniteIO``. The connectors are prefixed with
 _read_, _readAll_, _write_ or _delete_:

- _read_: Issue a single query to CDF and return the results.
- _readAll_: Issue multiple queries to CDF and return the results.
- _write_: Write items to CDF. The write operation will behave as an upsert.
- _delete_: Delete the specified items from CDF.

For example, ``CogniteIO.readAssets()`` will return a reader for the asset resource type.

#### Using readers

The readers issue one or more queries to Cognite Fusion and returns the result as a ``PCollection<T>`` of typed results items. The result item type depends on the CDF resource type (``readAssets()`` returns ``Asset``, ``readEvents()`` returns ``Event``, etc.).

Readers have a set of common configuration options:

- ``ProjectConfig``, mandatory. You must configure the reader with a Cognite tenant/project and api key. Optionally, you can also specify the host (for special cases when using a non-standard host).
- ``RequestParameters``, optional. ``RequestParameters`` host any query/request specification for the reader. For example, if you want to apply filters (e.g. list _events_ where _type=MyEventType_). If you do not supply any request parameters the reader will return all objects of the given resource type.
- ``Hints``, optional. ``Hints`` 

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/cognitedata/cdp-beam-connector-java.git)
