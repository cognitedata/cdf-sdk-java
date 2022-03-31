
# Files

A file stores a sequence of bytes connected to one or more assets. For example, a file can contain a piping and instrumentation diagram (P&IDs) showing how multiple assets are connected.

Each file is identified by the 'id' field, which is generated internally for each new file. Each file's 'id' field is unique within a project.

The 'externalId' field is optional, but can also be used to identify a file. The 'externalId' (if used) must be unique within a project.

Files are created in two steps; First the metadata is stored in a file object, and then the file contents are uploaded. This means that files can exist in a non-uploaded state. The upload state is reflected in the 'uploaded' field in responses.

> Note: To create client see [Client Setup](clientSetup.md)

### Upload file

Create metadata information and get an upload link for a file.

```java
Path fileAOriginal = Paths.get("./src/test/resources/csv-data.txt"); 
List<FileContainer> fileContainerInput = new ArrayList<>(); 
FileMetadata fileMetadata = FileMetadata.newBuilder() 
          .setExternalId("10") 
          .setName("test_file_.test") 
          .setSource("sdk-data-generator") 
          .putMetadata("type", "sdk-data-generator") 
     .build(); 

 FileContainer fileContainer = FileContainer.newBuilder() 
          .setFileMetadata(fileMetadata) 
          .setFileBinary(FileBinary.newBuilder() 
               .setBinaryUri(fileAOriginal.toUri().toString())) 
          .build(); 
 fileContainerInput.add(fileContainer); 

 List<FileMetadata> uploadFileResult = 
          client.files().upload(fileContainerInput); 
```

### Filter files

Retrieves a list of all files in a project.

```java
List<FileMetadata> listFilesResults = new ArrayList<>(); 
client.files() 
          .list() 
          .forEachRemaining(files -> listFilesResults.addAll(files)); 

client.files() 
          .list(Request.create() 
                .withFilterParameter("source", "sourceValue")) 
          .forEachRemaining(files -> listFilesResults.addAll(files));  
```

Options filter:
- filter:
    - name:
        - string (FileName) <= 256 characters
        - Name of the file.
    - directoryPrefix:
        - string <= 512 characters
        - Filter by this (case-sensitive) prefix for the directory.
    - mimeType:
        - string (MimeType) <= 256 characters
        - File type. E.g. text/plain, application/pdf, ..
    - metadata:
        - object (FilesMetadataField)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
    - assetIds:
        - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
        - Only include files that reference these specific asset IDs.
    - assetExternalIds:
        - Array of strings (AssetExternalIds) [ 1 .. 100 ] items unique [ items <= 255 characters ]
        - Only include files that reference these specific asset external IDs.
    - rootAssetIds:
        - Array of AssetInternalId (object) or AssetExternalId (object) (RootAssetIds) [ 1 .. 100 ] items unique [ items ]
        - Only include files that have a related asset in a tree rooted at any of these root assetIds.
        - One of:
        - AssetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - AssetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - assetSubtreeIds:
        - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
        - Only include files that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        - One of:
        - AssetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - AssetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - dataSetIds:
        - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
        - Only include files that belong to these datasets.
        - One of:
        - DataSetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - DataSetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - source:
        - string <= 128 characters
        - The source of this event.    
    - createdTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - lastUpdatedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - uploadedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - sourceCreatedTime:
      - object
      - Filter for files where the sourceCreatedTime field has been set and is within the specified range.
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - externalIdPrefix:
        - string (CogniteExternalIdPrefix) <= 255 characters
        - Filter by this (case-sensitive) prefix for the external ID.
    - uploaded:
        - boolean
        - Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
    - labels:
        - LabelContainsAnyFilter (object) or LabelContainsAllFilter (object) (LabelFilter)
        - Return only the resource matching the specified label constraints.
        - One of:
        - LabelContainsAnyFilter:
                - externalId An external ID to a predefined label definition.
                - string <= 255 characters
                - The resource item contains at least one of the listed labels.
        - LabelContainsAllFilter:
                - externalId An external ID to a predefined label definition.
                - string <= 255 characters
                - The resource item contains at least all the listed labels.
    - geoLocation:
        - object (GeoLocationFilter)
        - Only include files matching the specified geographic relation.
        - relation:
                - string
                - Enum: "INTERSECTS" "DISJOINT" "WITHIN"
                - One of the supported queries.
        - shape:
                - object
                - Represents the points, curves and surfaces in the coordinate space.
                - type
                    - string
                    - "Point" "MultiPolygon" "MultiLineString" "Polygon" "LineString"
                - coordinates
                    - Array of Array of numbers (LineStringCoordinates) >= 2 items [ items 2 items [ items ] ]
                    - Coordinates of a line described by a list of two or more points. Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.
                    - Example: [[30, 10], [10, 30], [40, 40]]


### Retrieve files

Retrieves metadata information about multiple specific files in the same project. 

```java
List<FileMetadata> retrievedFilesByExternalIds = client.files().retrieve("10");//by varargs of String 
List<Item> itemsExternalId = List.of(Item.newBuilder().setExternalId("10").build()); 
List<FileMetadata> resultsExternal = client.files().retrieve(itemsExternalId);//by list of items 

List<FileMetadata> retrievedFilesByInternalIds = client.files().retrieve(10, 20);//by varargs of Long 
List<Item> itemsInternalId = List.of(Item.newBuilder().setId(10).build()); 
List<FileMetadata> resultsInternal = client.files().retrieve(itemsInternalId);//by list of items 
```

### Delete files

Deletes the files with the given ids.

```java
List<Item> deleteByExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<Item> deleteItemsResults = client.files().delete(deleteByExternalIds); 

List<Item> deleteByInternalIds = List.of(Item.newBuilder() 
          .setId(10).build()); 
List<Item> deleteItemsResults = client.files().delete(deleteByInternalIds); 
```

### Download files

Retrieves a list of download URLs for the specified list of file IDs.

```java
List<Item> downloadByExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10").build()); 
List<FileContainer> downloadFilesResults = 
          client.files().downloadToPath(downloadByExternalIds, Paths.get("")); 

List<Item> downloadByInternallIds = List.of(Item.newBuilder() 
          .setId(10).build()); 
List<FileContainer> downloadFilesResults = 
          client.files().downloadToPath(downloadByInternallIds, Paths.get("")); 
```

### Update files

Updates the information for the files.

```java
List<FileMetadata> editFilesInput = listFilesResults.stream() 
 .map(fileMetadata -> fileMetadata.toBuilder() 
 .putMetadata("addedField", "new field value") 
 .build()) 
 .collect(Collectors.toList()); 

 List<FileMetadata> editFilesResult = 
          client.files().upsert(editFilesInput); 
```

### Aggregate files

Calculate aggregates for files, based on optional filter specification.

```java
Aggregate fileAggregate = 
          client.files().aggregate(Request.create() 
          .withFilterParameter("source", "source"));
```

Options filter:
- filter:
    - name:
        - string (FileName) <= 256 characters
        - Name of the file.
    - directoryPrefix:
        - string <= 512 characters
        - Filter by this (case-sensitive) prefix for the directory.
    - mimeType:
        - string (MimeType) <= 256 characters
        - File type. E.g. text/plain, application/pdf, ..
    - metadata:
        - object (FilesMetadataField)
        - Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
    - assetIds:
        - Array of integers `<int64>` (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
        - Only include files that reference these specific asset IDs.
    - assetExternalIds:
        - Array of strings (AssetExternalIds) [ 1 .. 100 ] items unique [ items <= 255 characters ]
        - Only include files that reference these specific asset external IDs.
    - rootAssetIds:
        - Array of AssetInternalId (object) or AssetExternalId (object) (RootAssetIds) [ 1 .. 100 ] items unique [ items ]
        - Only include files that have a related asset in a tree rooted at any of these root assetIds.
        - One of:
        - AssetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - AssetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - assetSubtreeIds:
        - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
        - Only include files that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        - One of:
        - AssetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - AssetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - dataSetIds:
        - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
        - Only include files that belong to these datasets.
        - One of:
        - DataSetInternalId:
                - id integer `int64` (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
        - DataSetExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.
    - source:
        - string <= 128 characters
        - The source of this event.    
    - createdTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - lastUpdatedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - uploadedTime:
      - object (EpochTimestampRange)
      - Range between two timestamps (inclusive).
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - sourceCreatedTime:
      - object
      - Filter for files where the sourceCreatedTime field has been set and is within the specified range.
      - max
                - integer `<int64>` >= 0
                - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min
                - integer `<int64>` >= 0
                - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - externalIdPrefix:
        - string (CogniteExternalIdPrefix) <= 255 characters
        - Filter by this (case-sensitive) prefix for the external ID.
    - uploaded:
        - boolean
        - Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
    - labels:
        - LabelContainsAnyFilter (object) or LabelContainsAllFilter (object) (LabelFilter)
        - Return only the resource matching the specified label constraints.
        - One of:
        - LabelContainsAnyFilter:
                - externalId An external ID to a predefined label definition.
                - string <= 255 characters
                - The resource item contains at least one of the listed labels.
        - LabelContainsAllFilter:
                - externalId An external ID to a predefined label definition.
                - string <= 255 characters
                - The resource item contains at least all the listed labels.
    - geoLocation:
        - object (GeoLocationFilter)
        - Only include files matching the specified geographic relation.
        - relation:
                - string
                - Enum: "INTERSECTS" "DISJOINT" "WITHIN"
                - One of the supported queries.
        - shape:
                - object
                - Represents the points, curves and surfaces in the coordinate space.
                - type
                    - string
                    - "Point" "MultiPolygon" "MultiLineString" "Polygon" "LineString"
                - coordinates
                    - Array of Array of numbers (LineStringCoordinates) >= 2 items [ items 2 items [ items ] ]
                    - Coordinates of a line described by a list of two or more points. Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.
                    - Example: [[30, 10], [10, 30], [40, 40]]