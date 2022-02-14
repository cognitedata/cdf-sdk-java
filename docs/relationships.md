## Relationships

The `relationship` resource type is a core structure of the Cognite Data Fusion graph model; it represents the `edges` 
in the graph while the other resource types (`time series`, `events`, `assets`, `files` and `sequences`.) represent 
the `nodes`. A `relationship` object can host attributes of its own, so you can describe the nature and validity 
of a link between two data objects. For example, you can add a time window to express that the relationship is only 
valid during a certain time. You can also set a confidence level to express the "strenght" of the link--this can be 
particularly useful when contextualizing data.

`Relationships` support all the usual operations described in [how to read and write data](readAndWriteData.md). 

> Note: To create client see the file [clientSetup.md](clientSetup.md)

### Upsert (create and edit)

One thing to be aware of, is how to specify the `source` and `target` of a `relationship`. When creating or updating 
a relationship you specify source and target by setting the appropriate `resource type` and `externalId`.
```java
// Build the relationship object
Relationship rel = Relationship.newBuilder()
        .setExternalId("my-external-id")
        .setSourceType(Relationship.ResourceType.EVENT)         // Specify the source resource type
        .setSourceExternalId("sap:wo:400_239292")               // The source externalId
        .setTargetType(Relationship.ResourceType.EVENT)         // Specify the target resource type
        .setTargetExternalId("sap:woItem:400_23434703")         // Specify the target externalId
        .setDataSetId(123456789L)                               // Optional: the internal id of the data set
        .setStartTime(1635604752000)                            // Optional: start time in epoch ms
        .setEndTime(1635605752000)                              // Optional: end time in epoch ms
        .setConfidence(1.0f)                                    // Optional: confidence level 0 - 1
        .addLabels("label:my-label")                            // Optional: add labels via externalId
        .build();

// Upsert the relationship to CDF
CogniteClient client = CogniteClient.ofClientCredentials(
        "my-client-id",
        "my-client-secret",
        TokenUrl.generateAzureAdURL("my-azure-ad-tenant-id"))
        .withProject("my-cdf-project")
        .withBaseUrl("https://my-cdf-host");                     // Optional
        
client.relationships()
        .upsert(List.of(rel));
```

### List and retrieve

When reading `relationships` via `list()` or `retrive()` you can choose to include the source and target objects
(not just their `externalId` references) in the response object. This can be useful when you need to inspect the 
nodes of the graph as a part of your traversal.

```java
// Listing relationships
List<Relationship> listRelationshipsResults = new ArrayList<>();
client.relationships()
        .list(Request.create()
                .withRootParameter("fetchResources", true))     // Add <"fetchResources", true> as a parameter to the 
        .forEachRemaining(listRelationshipsResults::addAll);    // request to include source and target objects.

// Retrieving relationships
Item relItem = Item.newBuilder()
        .setExternalId("external-id-of-relationship-object")
        .build();

List<Relationship> retrievedRelationshipsWithObjects = client
        .relationships()                                // When the second parameter is set to "true", source and target
        .retrieve(relationshipItems, true);             // objects are included in the result.

List<Relationship> retrievedRelationships = client
        .relationships()                                // With no second parameter, source and target
        .retrieve(relationshipItems);                   // objects are not included in the result.


// Resolving the source and taget objects
// You can always retrieve the reference externalId and resource type...
String sourceExternalId = retrievedRelationships.get(0).getSourceExternalId();
if (retrievedRelationships.get(0).getSourceType() == Relationship.ResourceType.ASSET) {   
    // continue resolving the asset or iterate further in the graph...
}

// When the source and target objects are included, you can access them directly.
// Since source/target are optimitically included, you should always double check their presence
// via one of the Relationship case methods. 
if (retrievedRelationshipsWithObjects.get(0).hasSourceAsset()) {                    // Check source object and type using
    Asset sourceAsset = retrievedRelationshipsWithObjects.get(0).getSourceAsset();  // the hasSourceXXX() and hasTargetXX()
}                                                                                   // methods

if (retrievedRelationshipsWithObjects.get(0).getSourceCase() == Relationship.SourceCase.SOURCE_NOT_SET) {
    // You can also check the source and target "case" to check if it is set--and its resource type
}

```

### Delete relationships

Delete the relationships between resources identified by the external IDs in the request.

```java

Item item = Item.newBuilder()
    .setExternalId("external-id-of-relationship-object")
    .build();

List<Item> deleteItemsResults = 
        client
        .relationships()
        .delete(List.of(item));
```