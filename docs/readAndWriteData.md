## Reading and writing data from/to Cognite Data Fusion

The Java SDK follows the Cognite Data Fusion (CDF) REST API structure, so it is helpful to familiarize yourself with 
the Cognite API documentation: [https://docs.cognite.com/api/v1/](https://docs.cognite.com/api/v1/). The SDK is 
structured into different sections that mirror the API endpoints (`assets`, `events`, `contextualization`, etc.). These 
sections host the operations you can access (`list`, `retrive`, `upsert`, etc.). 

Most operations will consume and/or return some data objects. The Java SDK uses typed objects based on Protobuf 
([https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers)). The structure 
of the data transfer objects follow the structure of the Cognite API objects. 

### Common data read/write operations

Most resource types support a core set of read/write operations:
- _List_. Returns all objects from CDF that match a filter--or all objects if no filter is specified.
- _Retrieve_. Returns objects from CDF based on specified `externalId` or `id`.
- _Aggregate_. Performes an aggregate operation (typically `count`) on data in CDF. 
- _Upsert_. Creates or updates objects in CDF.
- _Delete_. Removes objects from CDF based on specified `externalId` or `id`.

#### List

The `list` operation retrieves the set of data objects that match a specified filter. You specify the (optional) filter 
using the `Request` object.  If you don't specify a filter, all data objects will be returned. 

This operation will use parallel O/I for improved performance and return the results in a streaming fashion, 
one batch at a time. A single batch usually contains 1k - 8k objects. The client returns an `Iterator` to you so you control 
how to page through the entire results set. 

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// List events from a given source (filter by the event.source attribute)
List<Event> listEventsResults = new ArrayList<>();      // a container to host the results.
Request request = Request.create()
        .withFilterParameter("source", "mySourceValue");    // Set filter on attribute "source" to match the value "mySourceValue"

client.events()
        .list(request)    
        .forEachRemaining(eventBatch -> listEventsResults.addAll(eventBatch));        //results are read in batches

// List all events (no filter)
client.events()
        .list()                                                                       // no filter
        .forEachRemaining(eventBatch -> listEventsResults.addAll(eventBatch));        //results are read in batches
```

#### Retrieve

The `retrieve` operation returns objects based on `externalId` and/or `id`. You provide the ids as input via the 
`Item` object (). All matching objects are returned as a single batch to you, but behind the scenes the SDK may break 
the request up into multiple, parallel operations for improved performance. 

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// Retrieve assets by externalId
Item assetA = Item.newBuilder()
        .setExternalId("assetAExternalId")
        .build();

Item assetB = Item.newBuilder()
        .setExternalId("assetBExternalId")
        .build();

List<Asset> results = client.assets()
        .retrieve(List.of(assetA, assetB));
```

#### Aggregate

`Aggregate` performs an aggregate operation (for example, `count`) on a set of data objects. It operates per resource 
type (i.e. it cannot aggregate across resource types). The operation returns an `Aggregate` result object which contains 
the various results records. 

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// Count events from a given source (filter by the event.source attribute)
Request request = Request.create()
        .withFilterParameter("source", "mySourceValue");    // Set filter on attribute "source" to match the value "mySourceValue"

Aggregate aggregateResult = client.events()
        .aggregate(request);

System.out.println("Count of events: " + aggregateResult.getAggregates(0).getCount());
```

#### Upsert

The SDK combines the API's `create` and `update` operations into a single `upsert` operation. You don't need to worry 
about if an object already exists in CDF or not--the SDK will handle that for you. It will automatically perform the 
correct operation towards CDF. 

You can control the `upsert` behavior via a configuration setting: 1) `UpsertMode.UPDATE` or 2) `UpsertMode.REPLACE`. This 
setting only affects update behavior--not create. That is, if the object exists in CDF from before, then this setting 
controls how that object is updated. In mode `UPDATE` (the default mode) the SDK will update any provided object 
attributes--other previous attributes will remain unchanged. In mode `REPLACE`, all previous object attribute values will be 
removed and the provided attributes will be set.

`Upsert` uses the data objects' `externalId` to choose between a create and update operation. Therefore, we strongly 
recommend that you make it a practice to always populate the `externalId` attribute of all your data objects.

The SDK will automatically batch and parallelize the upsert operation for maximum performance.

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// Define the events (or other resource type) to write to CDF
Event eventA = Event.newBuilder()
        .setExternalId("myEvent:eventA")
        .setStartTime(Instant.parse("2021-01-01T01:01:01.00Z").toEpochMilli())
        .setEndTime(Instant.parse("2021-01-11T01:01:01.00Z").toEpochMilli())
        .setDescription("Change equipment")
        .setType("maintenance")
        .setSubtype("workorder")
        .setSource("myPlantMaintenanceSystem")
        .putMetadata("sourceId", "eventA")
        .build();

Event eventB = Event.newBuilder()
        .setExternalId("myEvent:eventB")
        .setStartTime(Instant.parse("2001-12-01T01:01:01.00Z").toEpochMilli())
        .setDescription("Equipment fault")
        .setType("maintenance")
        .setSubtype("notification")
        .setSource("myPlantMaintenanceSystem")
        .putMetadata("sourceId", "eventB")
        .build();

// write the events to CDF
client.events()
        .upsert(List.of(eventA, eventB));

// set the update mode to REPLACE and write new event
client = client
        .withClientConfig(ClientConfig.create()
                .withUpsertMode(UpsertMode.REPLACE));

Event newEventB = Event.newBuilder()
        .setExternalId("myEvent:eventB")
        .setStartTime(Instant.parse("2001-12-11T01:01:01.00Z").toEpochMilli())
        .setDescription("Equipment slow response")
        .setType("maintenance")
        .setSubtype("notification")
        .setSource("myPlantMaintenanceSystem")
        .putMetadata("sourceId", "eventB")
        .build();

client.events()
        .upsert(List.of(newEventB));
```

#### Delete

The `delete` operation will remove data objects from CDF based on their `externalId` or `id`. You specify which CDF 
objects to remove via the generic `Item` object (which encapsulates `externalId`/`id`).

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// Delete assets by externalId
Item assetA = Item.newBuilder()
        .setExternalId("assetAExternalId")
        .build();

Item assetB = Item.newBuilder()
        .setExternalId("assetBExternalId")
        .build();

client.assets()
        .delete(List.of(assetA, assetB));
```

### The request object

The `Request` object is a core object used throughout the SDK to encapsulate a (CDF) request payload. You use 
`Request` to specify the request payload for operations that accepts complex input, such as `list` and `aggregate`. 
These operations allow complex filter expressions (please refer to 
[the API documentation](https://docs.cognite.com/api/v1/) for reference). 

You have several options for how to configure a `Request`:
- Use the convenience methods `withRootParameter(String, Object)`, `withFilterParameter(String, Object)`, 
  `withFilterMetadataParameter(String, String)`, etc. This is the most common pattern as it allows you to easily 
  specify individual request parameters without having to know the internal structure of the request body.
- Use `withRequestParameters(Map<String, Object>)` to specify the entire request body using Java objects. In this case you use Java objects 
to represent a Json request body. The mapping is fairly straight forward with `Map<String, Object>` -> `Json object`, 
  `List<T>` -> `Json array`, and `Java litterals` -> `Json litterals`.
- Use `withRequestJson(String)` to specify the entire request body using a valid Json string.

```java
// Build a request for listing events using the convenience methods
Request myRequest = Request.create()
        .withFilterParameter("type", "notification")
        .withMetadataFilterParameter("myMetadataField", "myMetadataValue");

// Some more advanced examples. First, a complex query for assets
Request myRequest = Request.create()
    .withRootParameter("aggregatedProperties", List.of("path", "depth"))        // Json array is represented by List
    .withFilterParameter("source", "mySourceSystem")
    .withFilterParameter("labels", Map.of("containsAny", List.of(               // Json object with an array of objects
            Map.of("externalId", "labelA"),                                     // becomes a Java Map with a List
            Map.of("externalId", "labelB")                                      // of Maps
        )))
    .withMetadataFilterParameter("myMetadataField", "myMetadataValue");


// A request for time series data points, specifying aggregates and filters
Request myRequest = Request.create()
        .withItems(List.of(                                                     // The "items" payload node 
                Map.of("externalId", "timeSeriesA"),                            // Json array of objects becomes a
                Map.of("externalId", "timeSeriesB")                             // Java List of Maps.
        ))
        .withRootParameter("start", "2w-ago")
        .withRootParameter("end", Instant.now().toEpochMilli())
        .withRootParameter("aggregatedProperties", List.of("path", "depth"))        // Json array is represented by List
        .withRootParameter("granularity", "h")
        ;

```

### Data transfer objects (resource types)

aadfgetdg

### Migrating from SDK <0.9.9

Previous to version 0.9.9 the proto objects used a workaround to represent `optional` attributes. 