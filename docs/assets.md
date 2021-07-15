## Assets

The `asset` resource type is the backbone of the Cognite data model; it is the core data structure that all other
resource types (`time series`, `events`, `3D`, `files`, etc.) link to--and thereby enables the contextual data 
experience.

`Assets` are often organized as a hierarchy to provide a breakdown structure that is easy to navigate. In order to use
the hierarchy capability of `assets`, you also have to comply with an extra set of constraints:
- A hierarchy can only have a single root node--represented by an `asset` without a `parentId`/`parentExternalId` reference.
- All (non-root) `assets` must reference a valid parent.
- No cyclical paths or self-references are allowed.

These constraints must be respected when performing operations on assets. In particular, it can be a bit tricky 
to create and/or update a hierarchy. For example, all create and update operations must be performed in topological 
order. The SDK has a number of methods that help you when operating on asset hierarchies:
- _Synchronize hierarchy_. The SDK does most (if not all) of the work for you.
- _Upsert_. Safely add or update assets. 
- _Delete_. Remove assets, including recursive delete.
- _Verify hierarchy integrity_. Checks if your input satisfies all data integrity constraints.

### Synchronize hierarchy
The SDK can handle all hierarchy operations for you via `synchronizeHierarchy(Collection<Asset> assetHierarchy)`. Using 
this method, you input the target state of the hierarchy (via a collection of `assets`), and the SDK does the rest for
you--it verifies the input, performs change detection to optimize operations and operates the assets in the 
correct order. 

```java
List<Asset> myInputAssets = new ArrayList<>();
populateMyAssets(myInputAssets)  // Fill the collection with all assets
List<Asset> upsertedAssets = cogniteClient.assets().synchronizeHierarchy(myInputAssets);
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
