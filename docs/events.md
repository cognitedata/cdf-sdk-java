
# Events

Event objects store complex information about multiple assets over a time period. For example, an event can describe two hours of maintenance on a water pump and some associated pipes, or a future time window where the pump is scheduled for inspection. This is in contrast with data points in time series that store single pieces of information about one asset at specific points in time (e.g., temperature measurements).

An event’s time period is defined by a start time and end time, both millisecond timestamps since the UNIX epoch. The timestamps can be in the future. In addition, events can have a text description as well as arbitrary metadata and properties.

> Note: To create client see [Client Setup](clientSetup.md)

### Create events

Creates multiple event objects in the same project

```java
List<Event> upsertEventsList = List.of(Event.newBuilder() 
          .setExternalId("10") 
          .setStartTime(1552566113) 
          .setEndTime(1553566113) 
          .setDescription("generated_event_") 
          .setType("generated_event") 
          .setSubtype("event_sub_type") 
          .setSource("sdk-data-generator") 
          .putMetadata("type", "sdk-data-generator") 
     .build()); 
client.events().upsert(upsertEventsList); 
```
 
### Filter all events

Retrieve a list of all events in the same project. 

```java
List<Event> listEventsResults = new ArrayList<>(); 
client.events() 
          .list() 
          .forEachRemaining(events -> listEventsResults.addAll(events)); 

client.events() 
          .list(Request.create() 
               .withFilterParameter("source", "source")) 
          .forEachRemaining(events -> listEventsResults.addAll(events)); 
```

Options filter:
- filter:
    - startTime:
        - object (EpochTimestampRange)
        - Range between two timestamps (inclusive).
        - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - endTime:
      - EpochTimestampRange (object) or IsNull (object) (EndTimeFilter)
      - Either range between two timestamps or isNull filter condition.
      - One Of:
            - EpochTimestampRange:
                - max
                        - integer `<int64>` >= 0
                        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                - min
                        - integer `<int64>` >= 0
                        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
            - IsNull:
                - IsNull
                        - boolean
                        - Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
    - activeAtTime:
        - object (ActiveAtTimeFilter)
        - Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - metadata:
        - object (EventMetadata)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
    - assetIds:
        - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 5000 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
        - Asset IDs of equipment that this event relates to.
    - assetExternalIds:
        - Array of strings (CogniteExternalId) [ 1 .. 5000 ] items unique [ items <= 255 characters ]
        - Asset External IDs of equipment that this event relates to.
    - assetSubtreeIds:
      - Array of AssetInternalId (object) or AssetExternalId (object) (AssetIdEither) [ 1 .. 100 ] items unique [ items ]
      - Only include events that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
      - One Of:
            - AssetInternalId:
                - id
                        - integer `<int64>` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                        - A server-generated ID for the object.                
            - AssetExternalId:
                - externalId
                        - string (CogniteExternalId) <= 255 characters
                        - The external ID provided by the client. Must be unique for the resource type.
    - dataSetIds:
      - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
      - One Of:
            - DataSetInternalId:
                - id
                        - integer `<int64>` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                        - A server-generated ID for the object.                
            - DataSetExternalId:
                - externalId
                        - string (CogniteExternalId) <= 255 characters
                        - The external ID provided by the client. Must be unique for the resource type.
    - source:
        - string (EventSource) <= 128 characters
        - The source of this event.
    - type:
        - string (EventType) <= 64 characters
        - Type of the event, e.g 'failure'.
    - subtype:
        - string (EventSubType) <= 64 characters
        - SubType of the event, e.g 'electrical'.
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

 
### Aggregate events

The aggregation API allows you to compute aggregated results on events like getting the count of all events in a project or checking what are all the different types and subtypes of events in your project, along with the count of events in each of those aggregations. By specifying an additional filter, you can also aggregate only among events matching the specified filter.

The default behavior, when you do not specify the aggregate field in the request body, is to return the count of events.

 ```java
Aggregate aggregateResult = 
          client.events().aggregate(Request.create().withFilterParameter("source", "source"));
```

Options filter:
- filter:
    - startTime:
        - object (EpochTimestampRange)
        - Range between two timestamps (inclusive).
        - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - endTime:
      - EpochTimestampRange (object) or IsNull (object) (EndTimeFilter)
      - Either range between two timestamps or isNull filter condition.
      - One Of:
            - EpochTimestampRange:
                - max
                        - integer `<int64>` >= 0
                        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                - min
                        - integer `<int64>` >= 0
                        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
            - IsNull:
                - IsNull
                        - boolean
                        - Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
    - activeAtTime:
        - object (ActiveAtTimeFilter)
        - Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - metadata:
        - object (EventMetadata)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
    - assetIds:
        - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 5000 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
        - Asset IDs of equipment that this event relates to.
    - assetExternalIds:
        - Array of strings (CogniteExternalId) [ 1 .. 5000 ] items unique [ items <= 255 characters ]
        - Asset External IDs of equipment that this event relates to.
    - assetSubtreeIds:
      - Array of AssetInternalId (object) or AssetExternalId (object) (AssetIdEither) [ 1 .. 100 ] items unique [ items ]
      - Only include events that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
      - One Of:
            - AssetInternalId:
                - id
                        - integer `<int64>` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                        - A server-generated ID for the object.                
            - AssetExternalId:
                - externalId
                        - string (CogniteExternalId) <= 255 characters
                        - The external ID provided by the client. Must be unique for the resource type.
    - dataSetIds:
      - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
      - One Of:
            - DataSetInternalId:
                - id
                        - integer `<int64>` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                        - A server-generated ID for the object.                
            - DataSetExternalId:
                - externalId
                        - string (CogniteExternalId) <= 255 characters
                        - The external ID provided by the client. Must be unique for the resource type.
    - source:
        - string (EventSource) <= 128 characters
        - The source of this event.
    - type:
        - string (EventType) <= 64 characters
        - Type of the event, e.g 'failure'.
    - subtype:
        - string (EventSubType) <= 64 characters
        - SubType of the event, e.g 'electrical'.
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

### Retrieve events

Retrieves information about events in the same project. Events are returned in the same order as the ids listed in the query.

```java
List<Item> byExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<Event> resultByExternalIds = 
          client.events().retrieve(byExternalIds);//by list of items 
List<Event> resultByExternalIds = 
           client.events().retrieve("10", "20");//by varargs of String 

 List<Item> byInternalIds = List.of(Item.newBuilder() 
          .setId(10).build()); 
List<Event> resultByInternalIds = 
          client.events().retrieve(byInternalIds);//by list of items 
List<Event> resultByInternalIds = 
          client.events().retrieve(10, 20);//by varargs of Long
```

### Update events

Updates events in the same project. This operation supports partial updates; Fields omitted from queries will remain unchanged on objects.

```java
client.events().upsert(upsertEventsList); 
```

### Delete multiple events

Deletes events with the given ids. A maximum of 1000 events can be deleted per request.

```java
List<Item> byExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<Item> resultByExternalIds = 
          client.events().delete(byExternalIds); 

List<Item> byInternalIds = List.of(Item.newBuilder() 
           .setId(10).build()); 
List<Item> resultByInternalIds = 
          client.events().delete(byInternalIds); 
```