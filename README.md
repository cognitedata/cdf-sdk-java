[![java-main Actions Status](https://github.com/cognitedata/cdf-sdk-java/workflows/java-main/badge.svg)](https://github.com/cognitedata/cdf-sdk-java/actions)

<a href="https://cognite.com/">
    <img src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# Java sdk for CDF

Java SDK for reading and writing from/to CDF resources.

```java
// Create the Cognite client using API key as the authentication method
CogniteClient client = CogniteClient.ofKey(<yourApiKey>)
        .withBaseUrl("https://yourBaseURL.cognitedata.com");  //optional parameter
        
// ... or use client credentials (OpenID Connect)
CogniteClient client = CogniteClient.ofClientCredentials(
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
List<Events> listEventsResults = new ArrayList<>();
client.events()
        .list(Request.create()
                .withFilterParameter("key", "value"))       //optionally add filter parameters
        .forEachRemaining(eventBatch -> listEventsResults.addAll(eventBatch));        //results are read in batches

```
    
### Installing the sdk

```xml
<dependency>    
    <groupId>com.cognite</groupId>
    <artifactId>cdf-sdk-java</artifactId>
    <version>0.9.6</version>
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

#### Contextualization
- Entity matching
- Interactive P&ID (experimental)

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/cognitedata/cdf-sdk-java.git)
