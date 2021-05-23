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

Annotations can be detected via text matching. The  

Authentication via API key is the legacy method of authenticating services towards Cognite Data Fusion.
You simply supply the API key when creating the client:
```java
CogniteClient client = CogniteClient.ofKey(<apiKey>);
```