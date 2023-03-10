[![java-main Actions Status](https://github.com/cognitedata/cdf-sdk-java/workflows/java-main/badge.svg)](https://github.com/cognitedata/cdf-sdk-java/actions)

<a href="https://cognite.com/">
    <img src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# Java sdk for CDF

The Java SDK provides convenient access to Cognite Data Fusion's capabilities. It covers a large part of CDF's
capability surface, including experimental features. In addition, it is designed to handle a lot of the client "chores"
for you so you can spend more time on your core client logic.

Some of the SDK's capabilities:
- _Upsert support_. It will automatically handle `create`and `update` for you.
- _Streaming reads_. Subscribe to a stream of created and updated data.
- _Retries with backoff_. Transient failures will automatically be retried.
- _Performance optimization_. The SDK will handle batching and parallelization of requests.

Requirements SDK v2:
- Java 17

Requirements SDK v1:
- Java 11

Please refer to [the documentation](./docs/index.md) for more information: [./docs/index.md](./docs/index.md).

### Upcoming SDK v2

We have a new major version of the Java SDK in the pipeline. It is based on the v1 code line, but with a few breaking changes, so we bump it to a new major version. The main breaking changes include:

- Move to Java 17
- Remove deprecated methods from the SDK
- Refactor the diagram annotation data transfer object to accommodate the new `annotations` api endpoint.

It should not be too hard to move from v1 to v2 and we'll provide a migration guide for you. 

### Installing the sdk
SDK v2
```xml
<dependency>    
    <groupId>com.cognite</groupId>
    <artifactId>cdf-sdk-java</artifactId>
    <version>2.1.0</version>
</dependency>
```

SDK v1
```xml
<dependency>    
    <groupId>com.cognite</groupId>
    <artifactId>cdf-sdk-java</artifactId>
    <version>1.19.1</version>
</dependency>
```
    
### Features
#### Core resource types
- Time series
- Assets
- Events
- Files
- Sequences
- Relationships
- Raw

#### Data organization
- Data sets
- Labels
- Extraction Pipelines

#### Contextualization
- Entity matching
- Interactive P&ID

#### Login
- Login status by api-key

#### 3D
- 3D Models
- 3D Model Revisions
- 3D File Download
- 3D Asset Mapping

#### TRANSFORMATIONS
- Transformations
- Transformation Jobs
- Transformation Schedules
- Transformation Notifications

## Quickstart
```java        
// Create the Cognite client with client credentials (OpenID Connect)
CogniteClient client = CogniteClient.ofClientCredentials(
        <cdfProject>,
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter     
        
// List all assets
List<Asset> listAssetsResults = new ArrayList<>();
client.assets()
        .list(Request.create()
                .withFilterParameter("key", "value"))       //optionally add filter parameters
        .forEachRemaining(assetBatch -> listAssetsResults.addAll(assetBatch));        //results are read in batches

// List all events
List<Event> listEventsResults = new ArrayList<>();
client.events()
        .list(Request.create()
                .withFilterParameter("key", "value"))       //optionally add filter parameters
        .forEachRemaining(eventBatch -> listEventsResults.addAll(eventBatch));        //results are read in batches

```


[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/cognitedata/cdf-sdk-java.git)
