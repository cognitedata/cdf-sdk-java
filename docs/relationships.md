## Relationships

The `relationship` resource type is a core structure of the Cognite Data Fusion graph model; it represents the `edges` 
in the graph while the other resource types (`time series`, `events`, `assets`, `files` and `sequences`.) represent 
the `nodes`. A `relationship` object can host attributes of its own, so you can describe the nature and validity 
of a link between two data objects. For example, you can add a time window to express that the relationship is only 
valid during a certain time. You can also set a confidence level to express the "strenght" of the link--this can be 
particularly useful when contextualizing data.

`Relationships` support all the usual operations described in [how to read and write data](readAndWriteData.md). 

### Upsert (create and edit)

One thing to be aware of, is how to specify the `source` and `target` of a `relationship`. When creating or updating 
a relationship you specify source/target by setting the appropriate `resource type` and `externalId`.
```java
// Build the relationship object
Relationship rel = Relationship.newBuilder()
        .setExternalId("my-external-id")
        .setSourceType(Relationship.ResourceType.EVENT)         // Specify the source resource type
        .setSourceExternalId("sap:wo:400_239292")               // The source externalId
        .setTargetType(Relationship.ResourceType.EVENT)         // Specify the target resource type
        .setTargetExternalId("sap:woItem:400_23434703")         // Specify the target externalId
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
(not just their `externalId` references) in the response object. 

```java
List<Asset> myInputAssets = new ArrayList<>();
populateMyAssets(myInputAssets)  // Fill the collection with all assets

List<Asset> upsertedAssets = cogniteClient.assets().synchronizeMultipleHierarchies(myInputAssets);    // can be multiple hierarchies
        
List<Asset> upsertedAssets = cogniteClient.assets().synchronizeHierarchy(myInputAssets);        // only a single hierarchy
```

The first time you call the method it will build the hierarchy from scratch. Upon subsequent calls (with the same 
asset hierarchy as input), it will perform change detection and perform the needed operations (create, update, delete) 
to get the CDF hierarchy in sync with the input. 

The asset input collection must be a single, complete hierarchy satisfying the following constraints:
- All `assets` must specify an `externalId`.
- No duplicates (based on `externalId`).
- The collection must contain one and only one asset object with no parent reference (representing the root node).
- All other assets must contain a valid `parentExternalId` reference (no self-references).
- No circular references.

### Upsert
If you need to create and/or update a set of `assets` then the `upsert(Collection<Asset> assets)` method can 
come in handy. It will operate on the input in topological order and detect whether to perform a create or update 
operation. 

```java
List<Asset> myUpsertAssets = new ArrayList<>();
populateMyAssets(myUpsertAssets)  // Fill the collection with assets to upsert
List<Asset> upsertedAssets = cogniteClient.assets().upsert(myUpsertAssets);
```

The asset input collection must satisfy the following constraints:
- All `assets` must specify an `externalId`.
- No duplicates (based on `externalId`).
- All parent(External)Id references must point to either a) an existing asset in CDF or b) an asset in the input collection.
- No self-references.
- No circular references.

### Delete
You can delete `assets` via two flavors of `delete()`:
- `delete(List<Item> items)`. Deletes asset nodes in non-recursive mode. All items must be leaf nodes or complete sub-hierarchies.
- `delete(List<Item> items, boolean recursive)`. Deletes asset nodes in recursive mode.

Deleting assets in recursive mode is a fairly resource intensive operation. We recommend that you pay attention to the
complete size of the asset-hierarchy (or sub-tree) that will be impacted by the delete operation.

```java
List<Item> myDeleteItems = new ArrayList<>();
populateMyDeleteItems(myDeleteItems)  // Fill the collection with items to delete
List<Item> deletedItems = cogniteClient.assets().delete(myDeleteItems);
```

### Verify asset hierarchy integrity
You can ask the SDK to analyze an `asset` collection's data integrity without performing any operation towards CDF (no 
CRUD operations). This can be useful if you want to check if your asset collection represents a valid asset-hierarchy.

The SDK will report on any constraint violations via the log:
- All `assets` must specify an `externalId`.
- No duplicates (based on `externalId`).
- The collection must contain one and only one asset object with no parent reference (representing the root node).
- All other assets must contain a valid `parentExternalId` reference (no self-references).
- No circular references.

```java
List<Asset> myAssetHierarchy = new ArrayList<>();
populateMyAssets(myAssetHierarchy)  // Fill the collection with all assets
boolean isValid = cogniteClient.assets().verifyAssetHierarchyIntegrity(myInputAssets);
```
