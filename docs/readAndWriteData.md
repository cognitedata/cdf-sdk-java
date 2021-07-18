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
one batch at a time. A batch is usually 1k - 8k objects. The client returns an `Iterator` to you so you control 
how to page through the entire results set. 

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// List events from a given source (filter by the event.source attribute)
List<Event> listEventsResults = new ArrayList<>();      // a container to host the results.
Request request = Request.create()
        .withFilterParameter("source", "mySourceValue");    // Set filter on attribute "source" to match the value "mySourceValue"

client.events()
        .list(request)    
        .forEachRemaining(eventBatch -> listEventsResults.addAll(eventBatch));        //results are read in batches
```


### The request object

adfgadfg

### Data transfer objects (resource types)

aadfgetdg

### Migrating from SDK <0.9.9

Previous to version 0.9.9 the proto objects used a workaround to represent `optional` attributes. 