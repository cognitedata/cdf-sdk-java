
# Data sets

Data sets let you document and track data lineage, ensure data integrity, and allow 3rd parties to write their insights securely back to a Cognite Data Fusion (CDF) project.

Data sets group and track data by its source. For example, a data set can contain all work orders originating from SAP. Typically, an organization will have one data set for each of its data ingestion pipelines in CDF.

A data set consists of metadata about the data set, and the data objects that belong to the data set. Data objects, for example events, files, and time series, are added to a data set through the dataSetId field of the data object. Each data object can belong to only one data set.

To learn more about data sets, see [getting started guide](https://docs.cognite.com/cdf/data_governance/concepts/datasets/)

> Note: To create client see [Client Setup](clientSetup.md)

### Create data sets

Create metadata information and get an upload link for a file.

```java
List<DataSet> upsertDataSetList = List.of(DataSet.newBuilder() 
          .setExternalId("10") 
          .setName("generated-") 
          .setDescription("Generated description") 
          .putMetadata("type", "sdk-data-generator") 
          .putMetadata("source", "sdk-data-generator") 
          .build()); 

List<DataSet> upsertDataSetsResults = 
          client.datasets().upsert(upsertDataSetList); 
```

### Filter data sets

Use advanced filtering options to find data sets.

```java
List<DataSet> listDataSetsResults = new ArrayList<>(); 
client.datasets() 
          .list(Request.create()) 
          .forEachRemaining(batch -> listDataSetsResults.addAll(batch)); 
```

Options filter:
- filter:
    - metadata:
        - object (DataSetMetadata)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
    - createdTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - lastUpdatedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - externalIdPrefix:
        - string (CogniteExternalIdPrefix) <= 255 characters
        - Filter by this (case-sensitive) prefix for the external ID.
    - writeProtected:
        - boolean

### Aggregate data sets

Aggregate data sets in the same project. Criteria can be applied to select a subset of data sets.

```java
Aggregate aggregate = client 
          .datasets() 
          .aggregate(Request.create() 
          .withFilterParameter("source","sdk-data-generator")); 
```

Options filter:
- filter:
    - metadata:
        - object (DataSetMetadata)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
    - createdTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - lastUpdatedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - externalIdPrefix:
        - string (CogniteExternalIdPrefix) <= 255 characters
        - Filter by this (case-sensitive) prefix for the external ID.
    - writeProtected:
        - boolean

### Retrieve data sets

Retrieve data sets by IDs or external IDs.

```java
List<Item> retrieveByExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10") 
          .build()); 
List<DataSet> retrieveDataSetResults = client.datasets() 
          .retrieve(retrieveByExternalIds);//by list of items 
List<DataSet> retrieveDataSetResults = client.datasets() 
          .retrieve("10", "20");//by varargs of String 

List<Item> retrieveByInternalIds = List.of(Item.newBuilder() 
          .setId(10) 
          .build()); 
List<DataSet> retrieveDataSetResults = client.datasets() 
          .retrieve(retrieveByInternalIds);//by list of items 
List<DataSet> retrieveDataSetResults = client.datasets() 
          .retrieve(10, 20);//by varargs of Long 
```

### Update the attributes of data sets.


```java
List<DataSet> upsertDataSetList = //list of DataSet; 
List<DataSet> upsertDataSetsResults = 
          client.datasets().upsert(upsertDataSetList);
```