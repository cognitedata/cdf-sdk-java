## Custom HTTP requests

You can define custom HTTP requests to be issued to Cognite Data Fusion. The SDK will handle authentication, re-tries and throttling for you, while the `URI`, `headers` (optional) and `request body` are controlled by the client. The `response` is returned unprocessed and must be parsed/interpreted by the client.

This allows you both to 1) test early access API endpoint before the explicit SDK support and 2) use API endpoints that are not covered by the SDK. 

#### Build a POST request
Most of the Cognite Data Fusion api endpoints accepts a `POST` request carrying a `JSON` request body. You can craft the request body via a `String` in valid Json format, via the Java object inspired `Request` type or via the Protobuf Json equivalent `Struct`type. 

```java
// Build the cognite client
String clientId = "my-client-id";
String clientSecret = "my-client-secret";   // Remember to source this value from a secure transfer mechanism.
                                            // For example, via secret mapped to an environment variable.
String azureAdTenantId = "my-aad-tenant-id";
String cdfProject = "my-cdf-project";

CogniteClient client = CogniteClient.ofClientCredentials(
        cdfProject,
        clientId,
        clientSecret,
        TokenUrl.generateAzureAdURL(azureAdTenantId));

// Build the request URI. Look at the CDF API docs for valid URIs. In this example, we'll target 
// the "create event" api endpoint.
String cdfHost = "https://api.cognitedata.com";
String cdfApiEndpoint = "events";
URI requestURI = URI.create(String.format("%s/api/v1/projects/%s/%s",
        cdfHost,
        cdfProject,
        cdfApiEndpoint));

// Build the request body via string
String postBody = """
        {
            "items": [
                {
                    "externalId": "id-number-one",
                    "startTime": 1000000,
                    "endTime": 1000345,
                    "description": "test event",
                    "type": "generated_event",
                    "source": "test-event",
                    "metadata": {
                        "type": "test-event"
                    }
                },
                {
                    "externalId": "id-number-two",
                    "startTime": 1000000,
                    "endTime": 1000345,
                    "description": "test event-2",
                    "type": "generated_event",
                    "source": "test-event",
                    "metadata": {
                        "type": "test-event"
                    }
                }
            ]
        }
        """;

// Issue the HTTP post request
ResponseBinary responseBinary = client.experimental().cdfHttpRequest(requestURI)
        .withRequestBody(postBody)
        .post();

// Check that the request was successfull
if (responseBinary.getResponse().isSuccessful()) {
    String responseBody = responseBinary.getResponseBodyBytes().toStringUtf8();
} else {
    // handle the exception / invalid respose
}
```

You can build the request body via Java objects by using the `Request` object.
You have several options for how to configure a `Request`:
- Use the convenience methods `withRootParameter(String, Object)`, `withFilterParameter(String, Object)`, `withFilterMetadataParameter(String, String)`, etc. This is the most common pattern as it allows you to easily specify individual request parameters without having to know the internal structure of the request body.
- Use `withRequestParameters(Map<String, Object>)` to specify the entire request body using Java objects. In this case you use Java objects to represent a Json request body. The mapping is fairly straight forward with `Map<String, Object>` -> `Json object`, `List<T>` -> `Json array`, and `Java literals` -> `Json literals`.
- Use `withRequestJson(String)` to specify the entire request body using a valid Json string.
```java
// Build the create events request body via Java objects.
// Build the request Json root object body as a Java Map
Map<String, Object> requestBodyObject = new HashMap<>();

// Our items array is represented by a Java List
List<Map<String, Object>> itemsList = new ArrayList<>();
requestBodyObject.put("items", itemsList);

// Add event Json objects (represented by Java Map) to the list
Map<String, Object> eventA = new HashMap<>();
eventA.put("externalId", "id-number-one");
eventA.put("startTime", 1000000);
eventA.put("endTime", 1000345);
eventA.put("description", "test event");
eventA.put("type", "generated_event");
eventA.put("source", "test-event");
eventA.put("metadata", Map.of("type", "test-event"));
itemsList.add(eventA);

Map<String, Object> eventB = new HashMap<>();
eventB.put("externalId", "id-number-two");
eventB.put("startTime", 1000000);
eventB.put("endTime", 1000345);
eventB.put("description", "test event-2");
eventB.put("type", "generated_event");
eventB.put("source", "test-event");
eventB.put("metadata", Map.of("type", "test-event"));
itemsList.add(eventB);

// Add the request body to the request object
Request postEventsRequest = Request.create()
        .withRequestParameters(requestBodyObject);

// Issue the HTTP post request
ResponseBinary responseBinary = client.experimental().cdfHttpRequest(requestURI)
        .withRequestBody(requestBodyObject)
        .post();
```

Lastly, you can build the request body via protobuf `Struct` and `Value` objects:
```java
// Build the request Json object body as a Protobuf Struct
Struct.Builder requestBodyObjectBuilder = Struct.newBuilder();

// Our items array is represented by a list of Values
List<Value> itemsList = new ArrayList<>();

// Add event Json objects (represented by Struct) to the list
Struct eventA = Struct.newBuilder()
        .putFields("externalId", Values.of("id-number-tt"))
        .putFields("startTime", Values.of(1000000))
        .putFields("endTime", Values.of(1000345))
        .putFields("description", Values.of("test event"))
        .putFields("type", Values.of("generated_event"))
        .putFields("source", Values.of("test-event"))
        .putFields("metadata", Values.of(Structs.of("type", Values.of("test-event"))))
        .build();
itemsList.add(Values.of(eventA));

Struct eventB = Struct.newBuilder()
        .putFields("externalId", Values.of("id-number-yy"))
        .putFields("startTime", Values.of(1000000))
        .putFields("endTime", Values.of(1000345))
        .putFields("description", Values.of("test event-2"))
        .putFields("type", Values.of("generated_event"))
        .putFields("source", Values.of("test-event"))
        .putFields("metadata", Values.of(Structs.of("type", Values.of("test-event"))))
        .build();
itemsList.add(Values.of(eventB));

// Stitch all the objects together
requestBodyObjectBuilder.putFields("items", Values.of(itemsList));

ResponseBinary responseBinary = client.experimental().cdfHttpRequest(requestURI)
        .withRequestBody(requestBodyObjectBuilder.build())
        .post();                
```