# Time series

A time series consists of a sequence of data points connected to a single asset.

For example: A water pump asset can have a temperature time series that records a data point in units of °C every second.

A single asset can have several time series. The water pump could have additional time series measuring pressure within the pump, rpm, flow volume, power consumption, and more.

Time series store data points as either number or strings. This is controlled by the is_string flag on the time series object. Numerical data points can be aggregated before they are returned from a query (e.g., to find the average temperature for a day). String data points, on the other hand, cannot be aggregated by CDF, but can store arbitrary information like states (e.g. “open”/”closed”) or more complex information (JSON).

Cognite stores discrete data points, but the underlying process measured by the data points can vary continuously. When interpolating between data points, we can either assume that each value stays the same until the next measurement, or that it linearly changes between the two measurements. This is controlled by the is_step flag on the time series object. For example, if we estimate the average over a time containing two data points, the average will either be close to the first (is step) or close to the mean of the two (not is step).

A data point stores a single piece of information, a number or a string, associated with a specific time. Data points are identified by their timestamps, measured in milliseconds since the unix epoch -- 00:00, January 1st, 1970. Milliseconds is the finest time resolution supported by CDF i.e. fractional milliseconds are not supported. Leap seconds are not counted.

Numerical data points can be aggregated before they are retrieved from CDF. This allows for faster queries by reducing the amount of data transferred. You can aggregate data points by specifying one or more aggregates (e.g. average, minimum, maximum) as well as the time granularity over which the aggregates should be applied (e.g. “1h” for one hour).

Aggregates are aligned to the start time modulo the granularity unit. For example, if you ask for daily average temperatures since Monday afternoon last week, the first aggregated data point will contain averages for Monday, the second for Tuesday, etc. Determining aggregate alignment without considering data point timestamps allows CDF to pre-calculate aggregates (e.g. to quickly return daily average temperatures for a year). As a consequence, aggregating over 60 minutes can return a different result that aggregating over 1 hour because the two queries will be aligned differently.

Asset references obtained from a time series - through its asset id - may be invalid, simply by the non-transactional nature of HTTP. They are maintained in an eventual consistent manner.

> Note: To create client see [Client Setup](clientSetup.md)

### Create time series

Create one or more time series.

```java
List<TimeseriesMetadata> upsertTimeseriesList = List.of(TimeseriesMetadata.newBuilder() 
   .setExternalId("10") 
   .setName("test_ts") 
   .setIsString(false) 
   .setIsStep(false) 
   .setDescription("Description") 
   .setUnit("TestUnits")
   .setUnitExternalId("testUnitExternalId")
   .putMetadata("type", "sdk-data-generator") 
   .putMetadata("sdk-data-generator", "sdk-data-generator") 
 .build()); 

client.timeseries().upsert(upsertTimeseriesList); 


```

### Retrieve time series

Retrieve one or more time series by ID or external ID. The time series are returned in the same order as in the request.

```java
List<Item> byExternalIds = List.of(Item.newBuilder().setExternalId("10").build()); 
List<TimeseriesMetadata> retrievedAssets = client.timeseries().retrieve(byExternalIds);// by list of items 
List<TimeseriesMetadata> retrievedAssets = client.timeseries().retrieve("10", "20");// by varargs of String 

List<Item> byInternalIds = List.of(Item.newBuilder().setId(10).build()); 
List<TimeseriesMetadata> retrievedAssets = client.timeseries().retrieve(byInternalIds);// by list of items 
List<TimeseriesMetadata> retrievedAssets = client.timeseries().retrieve(10, 20);// by varargs of Long  
```

Options filter:
- items:
  - id
    - The external ID provided by the client. Must be unique for the resource type.
    - Array of integers `int64` [ 0 .. 100 ] items [ items &lt;int64&gt; ]
  - externalId
    - A server-generated ID for the object.
    - Array of integers `int64` [ 0 .. 100 ] items [ items &lt;int64&gt; ]
- ignoreUnknownIds:
    - boolean
    - Default: false
    - Ignore IDs and external IDs that are not found

### Filter time series

Retrieves a list of time series matching the specified criteria. This operation supports pagination by cursor. Criteria can be applied to select a subset of time series.

```java
List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>(); 
client.timeseries() 
 .list() 
 .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries)); 

List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>(); 
client.timeseries() 
 .list(Request.create() 
 .withFilterMetadataParameter("source", "source")) 
 .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries)); 

```

Options filter:
- filter:
  - name:
      - string
      - Filter on name.
  - unit:
      - string
      - Filter on unit.
  - isString:
      - boolean
      - Filter on isString.
  - isStep:
      - boolean
      - Filter on isStep.
  - metadata:
      - object (TimeSeriesMetadata)
      - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, up to 256 key-value pairs, of total size of at most 10000 bytes across all keys and values.
  - assetIds:
      - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Only include time series that reference these specific asset IDs.
  - assetExternalIds:
      - Array of strings (CogniteExternalId) [ 1 .. 100 ] items unique [ items <= 255 characters ]
      - Asset External IDs of related equipment that this time series relates to.
  - rootAssetIds:
      - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Only include time series that have a related asset in a tree rooted at any of these root assetIds.
  - assetSubtreeIds:
     - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
     - Only include sequences that belong to these datasets.
     - One of:
       - AssetInternalId:
              - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
              - A server-generated ID for the object.
       - AssetExternalId:
              - externalId string (CogniteExternalId) <= 255 characters
              - The external ID provided by the client. Must be unique for the resource type.
  - dataSetIds:
     - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
     - Only include sequences that belong to these datasets.
     - One of:
       - DataSetInternalId:
              - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
              - A server-generated ID for the object.
       - DataSetExternalId:
              - externalId string (CogniteExternalId) <= 255 characters
              - The external ID provided by the client. Must be unique for the resource type.
  - externalIdPrefix:
      - string (CogniteExternalIdPrefix) <= 255 characters
      - Filter by this (case-sensitive) prefix for the external ID.
  - createdTime:
    - integer `int64` (EpochTimestamp) >= 0 
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - lastUpdatedTime:
    - integer `int64` (EpochTimestamp) >= 0 
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.

### Aggregate time series

Count the number of time series that match the given filter

```java
Aggregate aggregateResult = client.timeseries() 
 .aggregate(Request.create() 
 .withFilterMetadataParameter("source", "source"));

```

Options filter:
- filter:
  - name:
      - string
      - Filter on name.
  - unit:
      - string
      - Filter on unit.
  - isString:
      - boolean
      - Filter on isString.
  - isStep:
      - boolean
      - Filter on isStep.
  - metadata:
      - object (TimeSeriesMetadata)
      - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, up to 256 key-value pairs, of total size of at most 10000 bytes across all keys and values.
  - assetIds:
      - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Only include time series that reference these specific asset IDs.
  - assetExternalIds:
      - Array of strings (CogniteExternalId) [ 1 .. 100 ] items unique [ items <= 255 characters ]
      - Asset External IDs of related equipment that this time series relates to.
  - rootAssetIds:
      - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Only include time series that have a related asset in a tree rooted at any of these root assetIds.
  - assetSubtreeIds:
     - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
     - Only include sequences that belong to these datasets.
     - One of:
       - AssetInternalId:
              - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
              - A server-generated ID for the object.
       - AssetExternalId:
              - externalId string (CogniteExternalId) <= 255 characters
              - The external ID provided by the client. Must be unique for the resource type.
  - dataSetIds:
     - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
     - Only include sequences that belong to these datasets.
     - One of:
       - DataSetInternalId:
              - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
              - A server-generated ID for the object.
       - DataSetExternalId:
              - externalId string (CogniteExternalId) <= 255 characters
              - The external ID provided by the client. Must be unique for the resource type.
  - externalIdPrefix:
      - string (CogniteExternalIdPrefix) <= 255 characters
      - Filter by this (case-sensitive) prefix for the external ID.
  - createdTime:
    - integer `int64` (EpochTimestamp) >= 0 
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - lastUpdatedTime:
    - integer `int64` (EpochTimestamp) >= 0 
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.

### Update time series

Updates one or more time series. Fields that are not included in the request, are not changed.

```java
client.timeseries().upsert(upsertTimeseriesList); 

```

### Delete time series

Deletes the time series with the specified IDs.

```java
List<Item> byInternalIds = List.of(Item.newBuilder().setId(10).build()); 
List<Item> deletedAssets = client.timeseries().delete(byInternalIds); 

List<Item> byExternalIds = List.of(Item.newBuilder().setExternalId("10").build()); 
List<Item> deletedAssets = client.timeseries().delete(byExternalIds); 

```

Options filter:
- ignoreUnknownIds
       - boolean
       - Default: false
       - Ignore IDs and external IDs that are not found


### Insert data points

Insert datapoints into a time series. You can do this for multiple time series. If you insert a datapoint with a timestamp that already exists, it will be overwritten with the new value.

```java
List<TimeseriesPointPost> items = new ArrayList<>(); items.add(TimeseriesPointPost.newBuilder() 
 .setExternalId("TimeseriesMetadata.id") 
 .setTimestamp(timeStamp.toEpochMilli()) 
 .setValueNum(ThreadLocalRandom.current().nextLong(-10, 20)) 
 .build()); 
client.timeseries().dataPoints().upsert(items); 

```

### Retrieve data points

Retrieves a list of data points from multiple time series in a project. This operation supports aggregation, but not pagination. A detailed description of how aggregates work can be found at [our concept guide for aggregation.](https://docs.cognite.com/dev/concepts/aggregation/)

```java
List<TimeseriesPoint> results = new ArrayList<>(); 

List<Item> byExternalIds = List.of(Item.newBuilder().setExternalId("10").build()); 
client.timeseries().dataPoints()
          .retrieveComplete(byExternalIds) 
          .forEachRemaining(result -> results.addAll(result));//by list of items  
client.timeseries().dataPoints()
          .retrieveComplete("10", "20") 
          .forEachRemaining(result -> results.addAll(result));//by varargs of String 

List<Item> byInternalIds = List.of(Item.newBuilder().setId(10).build()); 
client.timeseries().dataPoints()
          .retrieveComplete(byInternalIds) 
          .forEachRemaining(result -> results.addAll(result));//by list of items 
client.timeseries().dataPoints() 
          .retrieveComplete(10, 20) 
          .forEachRemaining(result -> results.addAll(result));//by varargs of Long 

//with filter
client.timeseries().dataPoints() 
          .retrieve(Request.create().withRootParameter("includeOutsidePoints", true)) 
          .forEachRemaining(items-> results.addAll(items)); 

```

Options filter:
- items:
     - Array of QueryWithInternalId (object) or QueryWithExternalId (object) (DatapointsQuery)
     - One of:
       - QueryWithInternalId:
              - start
                     - integer or string (TimestampOrStringStart)
                     - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
                     - One of:
                            - integer
                                   - Default: 0
                                   - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
                            - String
                                   - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
              - end
                     - integer or string (TimestampOrStringStart)
                     - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
                     - One of:
                            - integer
                                   Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
                            - String
                                   - Default: "now"
                                   - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
              - aggregates
                     - Array of strings (Aggregate) [ 0 .. 10 ] items unique
                     - Items Enum: "average" "max" "min" "count" "sum" "interpolation" "stepInterpolation" "totalVariation" "continuousVariance" "discreteVariance"
                     - Specify the aggregates to return. Use default if null. If the default is a set of aggregates, specify an empty string to get raw data.
              - granularity
                     - String
                     - The granularity size and granularity of the aggregates.
              - includeOutsidePoints
                     - boolean
                     - Default: false
                     - Whether to include the last datapoint before the requested time period,and the first one after. This option can be useful for interpolating data. It is not available for aggregates.
              - id
                     - integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
                     - A server-generated ID for the object.interpolating data. It is not available for aggregates.
       - QueryWithExternalId:
              - start
                     - integer or string (TimestampOrStringStart)
                     - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
                     - One of:
                            - integer
                                   - Default: 0
                                   - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
                            - String
                                   - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
              - end
                     - integer or string (TimestampOrStringStart)
                     - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
                     - One of:
                            - integer
                                   Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
                            - String
                                   - Default: "now"
                                   - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
              - aggregates
                     - Array of strings (Aggregate) [ 0 .. 10 ] items unique
                     - Items Enum: "average" "max" "min" "count" "sum" "interpolation" "stepInterpolation" "totalVariation" "continuousVariance" "discreteVariance"
                     - Specify the aggregates to return. Use default if null. If the default is a set of aggregates, specify an empty string to get raw data.
              - granularity
                     - String
                     - The granularity size and granularity of the aggregates.
              - includeOutsidePoints
                     - boolean
                     - Default: false
                     - Whether to include the last datapoint before the requested time period,and the first one after. This option can be useful for interpolating data. It is not available for aggregates.
              - externalId
                     - string (CogniteExternalId) <= 255 characters
                     - The external ID provided by the client. Must be unique for the resource type.

- start
       - integer or string (TimestampOrStringStart)
       - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
       - One of:
              - integer
                     - Default: 0
                     - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
              - String
                     - Get datapoints starting from, and including, this time. The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' gets datapoints that are up to 2 days old. You can also specify time in milliseconds since epoch. Note that for aggregates, the start time is rounded down to a whole granularity unit (in UTC timezone). Daily granularities (d) are rounded to 0:00 AM; hourly granularities (h) to the start of the hour, etc.
- end
       - integer or string (TimestampOrStringStart)
       - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
       - One of:
              - integer
                     Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
              - String
                     - Default: "now"
                     - Get datapoints up to, but excluding, this point in time. Same format as for start. Note that when using aggregates, the end will be rounded up such that the last aggregate represents a full aggregation interval containing the original end, where the interval is the granularity unit times the granularity multiplier. For granularity 2d, the aggregation interval is 2 days, if end was originally 3 days after the start, it will be rounded to 4 days after the start.
- aggregates
       - Array of strings (Aggregate) [ 0 .. 10 ] items unique
       - Items Enum: "average" "max" "min" "count" "sum" "interpolation" "stepInterpolation" "totalVariation" "continuousVariance" "discreteVariance"
       - Specify the aggregates to return. Use default if null. If the default is a set of aggregates, specify an empty string to get raw data.
- granularity
       - String
       - The granularity size and granularity of the aggregates.
- includeOutsidePoints
       - boolean
       - Default: false
       - Whether to include the last datapoint before the requested time period, and the first one after. This option is useful for interpolating data. It is not available for aggregates.
- ignoreUnknownIds
       - boolean
       - Default: false
       - Ignore IDs and external IDs that are not found

### Retrieve latest data point

Retrieves the latest data point in a time series.

```java
List<Item> byExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<TimeseriesPoint> result = 
          client.timeseries().dataPoints() 
          .retrieveLatest(byExternalIds);//by list of items 
List<TimeseriesPoint> result = 
          client.timeseries().dataPoints() 
          .retrieveLatest("10", "20");//by varargs of String 

List<Item> byInternalIds = List.of(Item.newBuilder() 
          .setId(10).build()); 
List<TimeseriesPoint> result = 
           client.timeseries().dataPoints() 
          .retrieveLatest(byInternalIds);//by list of items 
List<TimeseriesPoint> result = 
          client.timeseries().dataPoints() 
          .retrieveLatest(10, 20);//by varargs of Long 

```

### Delete datapoints

Delete datapoints from time series.

```java
List<Item> byExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<Item> deletedItems = 
           client.timeseries().dataPoints().delete(byExternalIds); 

List<Item> byInternalIds = List.of(Item.newBuilder() 
          .setId(10).build()); 
 List<Item> deletedItems = 
          client.timeseries().dataPoints().delete(byInternalIds); 



```