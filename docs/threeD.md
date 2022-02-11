## 3D 

> Note: To create client see the file [clientSetup.md](clientSetup.md)

### Models

#### List 3D models
Retrieves a list of all models in a project. This operation supports pagination. You can filter out all models without a published revision.

```java
List<ThreeDModel> listResults = new ArrayList<>();
client
    .threeD()
    .models()
    .list()
    .forEachRemaining(model -> listResults.addAll(model));
```

Options filter:
- published:
  - boolean
  - Filter based on whether or not it has published revisions.

```java
List<ThreeDModel> listResults = new ArrayList<>();

Request request = Request.create()
    .withRootParameter("published", "");
    
client.threeD()
    .models()
    .revisions()
    .list(request)
    .forEachRemaining(model -> listResults.addAll(model));
```

#### Create 3D models

```java
Long dataSetId = getOrCreateDataSet(client);

List<ThreeDModel> upsertThreeDModelsList = 
        DataGenerator.generate3DModels(1, dataSetId);

List<ThreeDModel> listUpsert = 
        client
        .threeD()
        .models()
        .upsert(upsertThreeDModelsList);

//Example to generate data of ThreeDModel
public static List<ThreeDModel> generate3DModels(int noObjects, long dataSetId) {
    List<ThreeDModel> objects = new ArrayList<>();
    for (int i = 0; i < noObjects; i++) {
      ThreeDModel.Builder builder = ThreeDModel.newBuilder();
      builder.setName("generated-" + RandomStringUtils.randomAlphanumeric(5));
      builder.setDataSetId(dataSetId);
      builder.setCreatedTime(1552566113 + ThreadLocalRandom.current().nextInt(10000));
      objects.add(builder.build());
    }
    return objects;
}

//Example to find data of DataSet
private Long getOrCreateDataSet(CogniteClient client) throws Exception {
    Request request = Request.create()
        .withRootParameter("limit", 1);
    Iterator<List<DataSet>> itDataSet = 
        client
        .datasets()
        .list(request);
    Long dataSetId = null;
    List<DataSet> list = itDataSet.next();
    dataSetId=list.get(0).getId();
}
```

#### Update 3D models

```java
List<ThreeDModel> listUpsert = find();

List<ThreeDModel> editedTdInput = 
        listUpsert.stream()
                .map(td -> td.toBuilder()
                        .setName("new-value")
                        .clearMetadata()
                        .putMetadata("new-key", "new-value")
                        .build())
                .collect(Collectors.toList());

List<ThreeDModel> tdUpdateResults = 
        client
        .threeD()
        .models()
        .upsert(editedTdInput);

```

#### Update 3D models Replace mode

When this mode is used, all values passed will be overwritten. If the object does not contain the value, it will be replaced by null, or it will be removed from a collection or map

```java
client = client
    .withClientConfig(ClientConfig.create()
    .withUpsertMode(UpsertMode.REPLACE));

List<ThreeDModel> tdReplaceResults = 
        client
        .threeD()
        .models()
        .upsert(editedTdInput);
```

#### Delete 3D models

```java

List<ThreeDModel> listUpsert = find();
List<Item> deleteItemsInput = new ArrayList<>();
listUpsert.stream()
        .map(td -> Item.newBuilder()
          .setId(td.getId())
          .build())
        .forEach(item -> deleteItemsInput.add(item));

List<Item> deleteItemsResults = 
        client
        .threeD()
        .models()
        .delete(deleteItemsInput);

```

#### Retrieve a 3D model

```java
List<ThreeDModel> listUpsert = find();
List<Item> tdList = new ArrayList<>();
listUpsert.stream()
  .map(td -> Item.newBuilder()
          .setId(td.getId())
          .build())
  .forEach(item -> tdList.add(item));

List<ThreeDModel> retrievedTD = 
        client
        .threeD()
        .models()
        .retrieve(tdList);

```

### Revisions

#### List 3D revisions
This operation returns a list of 3D revisions.

```java
List<ThreeDModelRevision> listResults = new ArrayList<>();
client.threeD()
    .models()
    .revisions()
    .list("threeDModelId")
    .forEachRemaining(model -> listResults.addAll(model));
```

Options filter:
- published:
    - boolean
    - Filter based on published status.

```java
List<ThreeDModelRevision> listResults = new ArrayList<>();

Request request = Request.create()
    .withRootParameter("published", "");
    
client.threeD()
    .models()
    .revisions()
    .list("threeDModelId", request)
    .forEachRemaining(model -> listResults.addAll(model));
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object

#### Retrieve a revision by ID
This operation returns a list of 3D revisions that have been requested with a list of ids
```java

List<Item> tdList = new ArrayList<>();
List.of(1L, 2L).stream()
    .map(id -> Item.newBuilder()
    .setId(id)
    .build())
    .forEach(item -> tdList.add(item));
        
List<ThreeDModelRevision> listResults = client.threeD()
    .models()
    .revisions()
    .retrieve("threeDModelId", tdList));
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object

#### Create or Update 3D revisions
This operation creates or updates 3D revisions. If your object contains id it will be updated otherwise it will be created

```java
FileMetadata file = create3DFile();
List<ThreeDModelRevision> upsertThreeDModelsList = generate3DModelsRevisions(2, file.getId());
List<ThreeDModelRevision> listUpsert =
        client.threeD()
        .models()
        .revisions()
        .upsert("threeDModelId", upsertThreeDModelsList);

//Example to generate data of ThreeDModelRevision
public static List<ThreeDModelRevision> generate3DModelsRevisions(int noObjects, long fileId) {
    Random random = new Random();
    List<ThreeDModelRevision> objects = new ArrayList<>();
    for (int i = 0; i < noObjects; i++) {
        ThreeDModelRevision.Builder builder = ThreeDModelRevision.newBuilder();
        
        ThreeDModelRevision.Camera.Builder cameraBuilder = ThreeDModelRevision.Camera.newBuilder();
        cameraBuilder.addPosition(2.707411050796509);
        cameraBuilder.addPosition(-4.514726638793945);
        cameraBuilder.addPosition(1.5695604085922241);
        cameraBuilder.addTarget(0.0);
        cameraBuilder.addTarget(-0.002374999923631549);
        cameraBuilder.addTarget(1.5695604085922241);
        
        builder.setFileId(fileId);
        builder.setCamera(cameraBuilder.build());
        builder.addRotation(random.nextInt(100) / 100.0);
        objects.add(builder.build());
    }
    return objects;
}

//Example to create 3d file
public static FileMetadata create3DFile() {
    byte[] fileByte = //File byte array
    List<FileMetadata> fileMetadataList = generateFileHeader3DModelsRevisions(1);
    List<FileContainer> fileContainerInput = new ArrayList<>();
    for (FileMetadata fileMetadata:  fileMetadataList) {
        FileContainer fileContainer = FileContainer.newBuilder()
        .setFileMetadata(fileMetadata)
        .setFileBinary(FileBinary.newBuilder()
        .setBinary(ByteString.copyFrom(fileByte)))
        .build();
        fileContainerInput.add(fileContainer);
    
        return client.files().upload(fileContainerInput);
    }
}

//Example to generate data of FileMetadata
public static List<FileMetadata> generateFileHeader3DModelsRevisions(int noObjects) {
    List<FileMetadata> objects = new ArrayList<>(noObjects);
    for (int i = 0; i < noObjects; i++) {
        objects.add(FileMetadata.newBuilder()
        .setExternalId(RandomStringUtils.randomAlphanumeric(10))
        .setName("NAME_FILE.obj")
        .setSource("sdk-data-generator")
        .putMetadata("type", "sdk-data-generator")
        .putMetadata("source", "sdk-data-generator")
        .build());
    }
    return objects;
}
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object

#### Delete 3D revisions
This operation deletes a list of 3D revisions.

```java
List<Item> deleteItemsInput = new ArrayList<>();
List.of(1L, 2L).stream()
    .map(id -> Item.newBuilder()
    .setId(id)
    .build())
    .forEach(item -> deleteItemsInput.add(item));
        
List<Item> deleteItemsResults = client.threeD()
    .models()
    .revisions()
    .delete("threeDModelId", deleteItemsInput);
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object

#### Update 3D revision thumbnail
This operation updates of 3D revision thumbnail.

```java
FileMetadata fileThumbnail = create3DFile();
Boolean updated = client.
    threeD()
    .models()
    .revisions()
    .updateThumbnail("threeDModelId", "threeDModelRevisionId", fileThumbnail.getId());

//Example to create 3d file
public static FileMetadata create3DFile() {
    byte[] fileByte = //File byte array
    List<FileMetadata> fileMetadataList = generateFile3DRevisionThumbnail(1);
    List<FileContainer> fileContainerInput = new ArrayList<>();
    for (FileMetadata fileMetadata:  fileMetadataList) {
        FileContainer fileContainer = FileContainer.newBuilder()
        .setFileMetadata(fileMetadata)
        .setFileBinary(FileBinary.newBuilder()
        .setBinary(ByteString.copyFrom(fileByte)))
        .build();
        fileContainerInput.add(fileContainer);
        
        return client.files().upload(fileContainerInput);
    }
}

//Example to generate data of FileMetadata
public static List<FileMetadata> generateFile3DRevisionThumbnail(int noObjects) {
List<FileMetadata> objects = new ArrayList<>(noObjects);
    for (int i = 0; i < noObjects; i++) {
        objects.add(FileMetadata.newBuilder()
            .setExternalId(RandomStringUtils.randomAlphanumeric(10))
            .setName("NAME_FILE.png")
            .setSource("sdk-data-generator")
            .setUploaded(true)
            .setMimeType("image/png")
            .putMetadata("type", "sdk-data-generator")
            .putMetadata("source", "sdk-data-generator")
            .build());
    }
return objects;
}

```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object


#### List 3D revision logs
This operation lists log entries for review

```java
List<ThreeDRevisionLog> listResults =
    client.threeD()
            .models()
            .revisions()
            .revisionLogs()
            .retrieve("threeDModelId", "threeDModelRevisionId");
```

Options filter:
- severity: 
  - integer int64
  - Default: 5
  - Minimum severity to retrieve (3 = INFO, 5 = WARN, 7 = ERROR).

```java
Request request = Request.create().withRootParameter("severity", Long.valueOf(3));
List<ThreeDRevisionLog> listResults =
    client.threeD()
            .models()
            .revisions()
            .revisionLogs()
            .retrieve("threeDModelId", "threeDModelRevisionId", request);
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object



#### List available outputs
This operation retrieves a list of available outputs for a processed 3D model. An output can be a format that can be consumed by a viewer (e.g. Reveal) or import in external tools. Each of the outputs will have an associated version which is used to identify the version of output format (not the revision of the processed output). Note that the structure of the outputs will vary and is not covered here.

```java
List<ThreeDOutput> listResults =
    client.threeD()
            .models()
            .revisions()
            .outputs()
            .retrieve("threeDModelId", "threeDModelRevisionId");
```

Options filter:
- format:
  - string
  - Format identifier, e.g. 'ept-pointcloud' (point cloud). Well known formats are: 'ept-pointcloud' (point cloud data), 'reveal-directory' (output supported by Reveal), 'nodes-json' (a JSON dump of all nodes in the file) and 'preview-glb' (a GLTF preview of the 3D model). In addition, 'all-outputs' can be provided to return all outputs. Note that many of the outputs are internal, where the format might change without any warning.

```java
Request request = Request.create().withRootParameter("format", "ept-pointcloud");
List<ThreeDOutput> listResults =
    client.threeD()
            .models()
            .revisions()
            .outputs()
            .retrieve("threeDModelId", "threeDModelRevisionId", request);
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object


#### List 3D nodes
This operation retrieves a list of nodes from the hierarchy in the 3D model. You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of the resulting subtree with the 'depth' query parameter. By default, nodes are returned in order of ascending treeIndex. We suggest trying to set the query parameter sortByNodeId to true to check whether it makes your use case faster. The partition parameter can only be used if sortByNodeId is set to true. This operation supports pagination.

```java
List<ThreeDNode> listResults = new ArrayList<>();
client.threeD()
    .models()
    .revisions()
    .nodes()
    .list("threeDModelId", "threeDModelRevisionId")
    .forEachRemaining(val -> listResults.addAll(val));
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object


#### Filter 3D nodes

List nodes in a project, filtered by node property values specified by supplied filters. This operation supports pagination and partitions.

Options filter:
- filter:
  - object (Node3DPropertyFilter)
  - Filters used in the search.
    - properties
      - object (Node3DPropertyFilter)
      - Contains one or more categories (namespaces), each of which contains one or more properties. Each property is associated with a list of values. The list of values acts as an OR-clause, so that if a node's corresponding property value equals ANY of the strings in the list, it satisfies the condition for that property. The different properties are concatenated with AND-operations, so that a node must satisfy the condition for ALL properties from all categories to be part of the returned set. The allowed number of property values is limited to 1000 values in total.

```java
Request request = Request.create()
        .withFilterParameter("properties", createFilterPropertiesWithCategories());

List<ThreeDNode> listResults = new ArrayList<>();
client.threeD()
    .models()
    .revisions()
    .nodes()
    .filter("threeDModelId", "threeDModelRevisionId", request)
    .forEachRemaining(val -> listResults.addAll(val));

//Example to generate data of filter
// This method will generate
// "properties": {"Item":{"Type":["Box"]}}
private ThreeDNode.PropertiesFilter createFilterPropertiesWithCategories() {
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValBuilder =
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
    catValBuilder.addValuesString("Box");
    
    ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
    catBuilder.setName("Item");
    catBuilder.putValues("Type", catValBuilder.build());
    
    ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
    propsBuilder.addCategories(catBuilder.build());
    return propsBuilder.build();
}
// This method will generate 
// "properties": {"Item":{"Type":["Group"], "Required":["false"]}, "PDMS":{"Type":["PNOD"]}}
private ThreeDNode.PropertiesFilter createFilterPropertiesWith2Categories() {
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValBuilder =
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
    catValBuilder.addValuesString("Group");
    
    ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
    catBuilder.setName("Item");
    catBuilder.putValues("Type", catValBuilder.build());
    
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder cat2ValBuilder =
    ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
    cat2ValBuilder.addValuesString("PNOD");
    
    ThreeDNode.PropertiesFilter.Categories.Builder cat2Builder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
    cat2Builder.setName("PDMS");
    cat2Builder.putValues("Type", cat2ValBuilder.build());
    
    ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
    propsBuilder.addCategories(catBuilder.build());
    propsBuilder.addCategories(cat2Builder.build());
    return propsBuilder.build();
}
```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object


#### Get 3D nodes by ID
Retrieves specific nodes given by a list of IDs.

```java
List<Item> idsList = new ArrayList<>();
        List.of(1L, 2L).stream()
        .map(id -> Item.newBuilder()
        .setId(id)
        .build())
        .forEach(item -> idsList.add(item));

List<ThreeDNode> nodesByIds = client.threeD()
        .models()
        .revisions()
        .nodes()
        .retrieve("threeDModelId", "threeDModelRevisionId", idsList);

```

PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object

#### List 3D ancestor nodes
Retrieves a list of ancestor nodes of a given node, including itself, in the hierarchy of the 3D model. This operation supports pagination.

```java
List<ThreeDNode> listResultsAncestorNodes = new ArrayList<>();
client.threeD()
        .models()
        .revisions()
        .nodes()
        .list("threeDModelId", "threeDModelRevisionI"d, "nodeId")
        .forEachRemaining(val -> listResultsAncestorNodes.addAll(val));
```

PS: 
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object
- Change the `nodeId` to id of ThreeDNode object

### Files

#### Retrieve a 3D file
Retrieve the contents of a 3D file.

This endpoint supported tag-based caching.

This endpoint is only compatible with 3D file IDs from the 3D API, and not compatible with file IDs from the Files API.

```java

client
    .threeD()
    .files()
    .downloadToPath("fileId", "fileTargetPath");

```

PS:
- Change the `fileId` to id of the 3D file to download.
- Change the `fileTargetPath` to URI to the download storage

### Asset mappings

#### List 3D asset mappings
List all asset mappings

Asset references obtained from a mapping - through asset ids - may be invalid, simply by the non-transactional nature of HTTP. They are NOT maintained by any means from CDF, meaning they will be stored until the reference is removed through the delete endpoint of 3d asset mappings.

```java
Iterator<List<ThreeDAssetMapping>> itFilter = 
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .list("threeDModelId", "threeDModelRevisionI");
```

Options filter:
- cursor:
  - string
  - Example: cursor=4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo
    Cursor for paging through results.
- limit:
  - integer [ 1 .. 1000 ]
  - Default: 100
  - Limits the number of results to be returned. The maximum results returned by the server is 1000 even if you specify a higher limit.
- nodeId:
  - integer <int64>
- assetId:
  - integer <int64>
- intersectsBoundingBox:
  - string
  - Example: {"min":[0.0, 0.0, 0.0], "max":[1.0, 1.0, 1.0]}
  - If given, only return asset mappings for assets whose bounding box intersects the given bounding box.
  - Must be a JSON object with min, max arrays of coordinates.

```java
Request request = Request.create()
        .withRootParameter("limit", 300)
        .withRootParameter("cursor", "4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo")
        .withRootParameter("nodeId", 1)
        .withRootParameter("assetId", 1)
        .withRootParameter("intersectsBoundingBox", createBoundingBox());

Iterator<List<ThreeDAssetMapping>> itFilter =
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .list("threeDModelId", "threeDModelRevisionI", request);

//Example to generate data of filter
// This method will generate
// {
//   "min":[62.64287567138672, 47.26144790649414, -74.95000457763672], 
//   "max":[214.71351623535156, 191.49485778808594, 125.31800079345703]
// }
public ThreeDNode.BoundingBox createBoundingBox() {
    ThreeDNode.BoundingBox.Builder builder = ThreeDNode.BoundingBox.newBuilder();
    builder.addMin(62.64287567138672);
    builder.addMin(47.26144790649414);
    builder.addMin(-74.95000457763672);
    builder.addMax(214.71351623535156);
    builder.addMax(191.49485778808594);
    builder.addMax(125.31800079345703);
    return builder.build();
}

```
PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object

#### Create 3D asset mappings
Create asset mappings

Asset references when creating a mapping - through asset ids - are allowed to be invalid. They are NOT maintained by any means from CDF, meaning they will be stored until the reference is removed through the delete endpoint of 3d asset mappings.

```java
List<ThreeDAssetMapping> items = new ArrayList<>();
ThreeDAssetMapping.Builder mappingBuilder = ThreeDAssetMapping.newBuilder();
mappingBuilder.setAssetId(1);
mappingBuilder.setNodeId(1);
items.add(mappingBuilder.build());

List<ThreeDAssetMapping> listCreated = 
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .create("threeDModelId", "threeDModelRevisionI", items);
```
PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object

#### Delete 3D asset mappings
Delete a list of asset mappings

```java
List<ThreeDAssetMapping> items = new ArrayList<>();
ThreeDAssetMapping.Builder mappingBuilder = ThreeDAssetMapping.newBuilder();
mappingBuilder.setAssetId(1);
mappingBuilder.setNodeId(1);
items.add(mappingBuilder.build());

Boolean isDeleted =
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .delete("threeDModelId", "threeDModelRevisionI", items);
```
PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object

#### Filter 3D asset mappings
Lists 3D assets mappings that match the specified filter parameter. Only one type of filter can be specified for each request, either assetIds, nodeIds or treeIndexes.

Asset references obtained from a mapping - through asset ids - may be invalid, simply by the non-transactional nature of HTTP. They are NOT maintained by any means from CDF, meaning they will be stored until the reference is removed through the delete endpoint of 3d asset mappings.

```java
Iterator<List<ThreeDAssetMapping>> itFilter = 
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .filter("threeDModelId", "threeDModelRevisionI");
```

Options filter:
- cursor:
  - string
  - Example: cursor=4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo
    Cursor for paging through results.
- limit:
  - integer [ 1 .. 1000 ]
  - Default: 100
  - Limits the number of results to be returned. The maximum results returned by the server is 1000 even if you specify a higher limit.
- filter:
  - assetIds
    - Array of integers <int64> [ 0 .. 100 ] items [ items &lt;int64&gt; ]
  - nodeIds
    - Array of integers <int64> [ 0 .. 100 ] items [ items &lt;int64&gt; ]
  - treeIndexes
    - Array of integers <int64> [ 0 .. 100 ] items [ items &lt;int64&gt; ]

```java
Request request = Request.create()
        .withRootParameter("limit", 100)
        .withRootParameter("cursor", "4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo")
        .withFilterParameter("assetIds", List.of(1, 2))
        .withFilterParameter("nodeIds", List.of(3, 4))
        .withFilterParameter("treeIndexes", List.of(5, 6));

Iterator<List<ThreeDAssetMapping>> itFilter = 
        client
        .threeD()
        .models()
        .revisions()
        .assetMappings()
        .filter("threeDModelId", "threeDModelRevisionI", request);
```
PS:
- Change the `threeDModelId` to id of ThreeDModel object
- Change the `threeDModelRevisionId` to id of ThreeDModelRevision object
