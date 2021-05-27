## Contextualization

The contextualization services of Cognite Data Fusion helps enrich and connect data. The services offer
different algorithms for information extraction/feature generation and matching. The SDK supports 
the following contextualization services:
- Entity matching.
- Interactive P&IDs.

### Entity matching

In order to use this flow
```java
CogniteClient client = CogniteClient.ofClientCredentials(
                    <clientId>,
                    <clientSecret>,
                    TokenUrl.generateAzureAdURL(<azureAdTenantId>));
```

### Interactive P&ID
The interactive P&ID service performs two main tasks:
1. Detect annotations.
2. Convert P&ID to an "interactive" SVG version

Annotations can be detected via text matching. You specify a set of `entities`to use as the basis for the matching. 
An entity can be specified as follows:
```java
Struct entity = Struct.newBuilder()
        .putFields("name", Values.of("1N914"))
        .putFields("resourceType", Values.of("Asset"))
        .putFields("externalId", Values.of("my-external-id-1"))
        .putFields("id", Values.of(146379580567867L))
        .build();
```


```java
CogniteClient client = CogniteClient.ofKey(<apiKey>);
```