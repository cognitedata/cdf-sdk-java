## Sequence

A sequence stores a table with up to 200 columns indexed by row number. Each of the columns has a pre-defined type which is a string, integer, or floating point number.

For example, a sequence can represent a curve, either with the dependent variable x as the row number and a single value column y, or can simply store (x,y) pair in the rows directly. Other potential applications include data logs in which the index is not time-based. To learn more about sequences, see the [concept guide](https://docs.cognite.com/dev/concepts/resource_types/sequences.html).

> Note: To create client see the file [clientSetup.md](clientSetup.md)

### Create sequences

Create one or more sequences.

```java
List<SequenceMetadata> upsertSequencesList = generateSequenceMetadata(10);
client
    .sequences()
    .upsert(upsertSequencesList);

//Example to generate data of SequenceMetadata
public static List<SequenceMetadata> generateSequenceMetadata(int noObjects) {
    List<SequenceMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            List<SequenceColumn> columns = new ArrayList<>();
            int noColumns = ThreadLocalRandom.current().nextInt(2,200);
            for (int j = 0; j < noColumns; j++) {
                columns.add(SequenceColumn.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(50))
                    .setName("test_column_" + RandomStringUtils.randomAlphanumeric(5))
                    .setDescription(RandomStringUtils.randomAlphanumeric(50))
                    .setValueTypeValue(ThreadLocalRandom.current().nextInt(0,2))
                    .build());
            }
        
        objects.add(SequenceMetadata.newBuilder()
            .setExternalId(RandomStringUtils.randomAlphanumeric(20))
            .setName("test_sequence_" + RandomStringUtils.randomAlphanumeric(5))
            .setDescription(RandomStringUtils.randomAlphanumeric(50))
            .putMetadata("type", DataGenerator.sourceValue)
            .putMetadata(sourceKey, DataGenerator.sourceValue)
            .addAllColumns(columns)
            .build());
        }
    return objects;
}

```

### Filter sequences

Retrieves a list of sequences matching the given criteria.

```java

List<SequenceMetadata> listSequencesResults = new ArrayList<>();
client
    .sequences()
    .list()
    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));

```

Options filter:
- filter:
  - name:
      - string
      - Return only sequences with this exact name.
  - externalIdPrefix:
      - string (CogniteExternalIdPrefix) <= 255 characters
      - Filter by this (case-sensitive) prefix for the external ID.
  - metadata:
      - object
      - Filter the sequences by metadata fields and values (case-sensitive). Format is `{"key1":"value1","key2":"value2"}`.
  - assetIds:
      - Array of integers <int64> (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Return only sequences linked to one of the specified assets.
  - rootAssetIds:
      - Array of integers <int64> (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
      - Only include sequences that have a related asset in a tree rooted at any of these root assetIds.
  - assetSubtreeIds:
      - Array of AssetInternalId (object) or AssetExternalId (object) (AssetIdEither) [ 1 .. 100 ] items unique [ items ]
      - Only include sequences that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
      - One of:  
        - AssetInternalId: 
          - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
          - A server-generated ID for the object.
        - AssetExternalId: 
          - externalId string (CogniteExternalId) <= 255 characters
          - The external ID provided by the client. Must be unique for the resource type.
  - createdTime:
    - object (EpochTimestampRange)
    - Range between two timestamps (inclusive).
      - max:
        - integer <int64> >= 0
        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min:
        - integer <int64> >= 0
        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - lastUpdatedTime:
    - object (EpochTimestampRange)
    - Range between two timestamps (inclusive).
      - max:
        - integer <int64> >= 0
        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min:
        - integer <int64> >= 0
        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - dataSetIds:
    - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
    - Only include sequences that belong to these datasets.
    - One of:
      - DataSetInternalId:
        - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
        - A server-generated ID for the object.
      - DataSetExternalId:
        - externalId string (CogniteExternalId) <= 255 characters
        - The external ID provided by the client. Must be unique for the resource type.

```java

List<SequenceMetadata> listSequencesResults = new ArrayList<>();

Request request = Request.create()
        .withFilterMetadataParameter("source", DataGenerator.sourceValue)
        .withFilterParameter("name","name"));

client
    .sequences()
    .list(request)
    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));

```

### Aggregate sequences

Count the number of sequences that match the given filter

Options filter:
- filter:
  - name:
    - string
    - Return only sequences with this exact name.
  - externalIdPrefix:
    - string (CogniteExternalIdPrefix) <= 255 characters
    - Filter by this (case-sensitive) prefix for the external ID.
  - metadata:
    - object
    - Filter the sequences by metadata fields and values (case-sensitive). Format is `{"key1":"value1","key2":"value2"}`.
  - assetIds:
    - Array of integers <int64> (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
    - Return only sequences linked to one of the specified assets.
  - rootAssetIds:
    - Array of integers <int64> (CogniteInternalId) [ 1 .. 100 ] items unique [ items &lt;int64&gt; [ 1 .. 9007199254740991 ] ]
    - Only include sequences that have a related asset in a tree rooted at any of these root assetIds.
  - assetSubtreeIds:
    - Array of AssetInternalId (object) or AssetExternalId (object) (AssetIdEither) [ 1 .. 100 ] items unique [ items ]
    - Only include sequences that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
    - One of:
      - AssetInternalId:
        - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
        - A server-generated ID for the object.
      - AssetExternalId:
        - externalId string (CogniteExternalId) <= 255 characters
        - The external ID provided by the client. Must be unique for the resource type.
  - createdTime:
    - object (EpochTimestampRange)
    - Range between two timestamps (inclusive).
      - max:
        - integer <int64> >= 0
        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min:
        - integer <int64> >= 0
        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - lastUpdatedTime:
    - object (EpochTimestampRange)
    - Range between two timestamps (inclusive).
      - max:
        - integer <int64> >= 0
        - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
      - min:
        - integer <int64> >= 0
        - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
  - dataSetIds:
    - Array of DataSetInternalId (object) or DataSetExternalId (object) (DataSetIdEither) <= 100 items unique [ items ]
    - Only include sequences that belong to these datasets.
    - One of:
      - DataSetInternalId:
        - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
        - A server-generated ID for the object.
      - DataSetExternalId:
        - externalId string (CogniteExternalId) <= 255 characters
        - The external ID provided by the client. Must be unique for the resource type.

```java

Request request = Request.create()
        .withFilterMetadataParameter("source", DataGenerator.sourceValue)
        .withFilterParameter("name","name"));

Aggregate aggregateResult = 
        client
        .sequences()
        .aggregate(request);

```

### Retrieve sequences

Retrieve one or more sequences by ID or external ID. The sequences are returned in the same order as in the request.

Options filter:
- items:
  - Array of Select by Id (object) or Select by ExternalId (object) [ 1 .. 1000 ] items [ items ]  
  - One of:
    - Select by Id:
      - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
      - A server-generated ID for the object.
    - Select by ExternalId:
      - externalId string (CogniteExternalId) <= 255 characters
      - The external ID provided by the client. Must be unique for the resource type.

```java
List<SequenceMetadata> listSequencesResults = find();

//By externalId
List<Item> sequencesItemsByExternalId = new ArrayList<>();
listSequencesResults.stream()
      .map(aequences -> Item.newBuilder()
              .setExternalId(aequences.getExternalId())
              .build())
      .forEach(item -> sequencesItemsByExternalId.add(item));

List<SequenceMetadata> retrievedSequencesByExternalId = 
      client
      .sequences()
      .retrieve(sequencesItemsByExternalId);

//By id
List<Item> sequencesItemsById = new ArrayList<>();
listSequencesResults.stream()
      .map(aequences -> Item.newBuilder()
            .setId(aequences.getId())
            .build())
       .forEach(item -> sequencesItemsById.add(item));

List<SequenceMetadata> retrievedSequencesById =
      client
      .sequences()
      .retrieve(sequencesItemsById);

```

### Update sequences

Update one or more sequences. The behavior of the update/upsert depends on the `upsert mode` of the client:
- `UpsertMode.UPDATE` (default): all configured sequence attributes will be updated in CDF. Attributes that are not configured will retain their original value. Any columns that are specified will either be created/added (in case of new columns) or updated (in case of existing columns). No columns will be deleted.
- `UpsertMode.REPLACE`: all configured sequence attributes will be updated in CDF. Attributes that are not configured will be removed/set to null. Any columns that are specified will either be created/added (in case of new columns) or updated (in case of existing columns). Existing columns that are not a part of the sequence object will be deleted.

```java
List<SequenceMetadata> upsertedSequences = find();

List<SequenceMetadata> editedSequencesInput = upsertedSequences.stream()
    .map(sequences -> sequences.toBuilder()
        .setDescription("new-value")
        .clearMetadata()
        .putMetadata("new-key", "new-value")
        .addColumns(SequenceColumn.newBuilder()
            .setExternalId("my-new-column-ext-id")
            .setName("new-column")
            .setDescription("A new column")
            .setValueType(SequenceColumn.ValueType.STRING)
            .putMetadata("column-metadata-key", "column-metadata-value")
            .build())
        .build())
    .collect(Collectors.toList());

List<SequenceMetadata> sequencesUpdateResults = 
        client
        .sequences()
        .upsert(editedSequencesInput);
```

### Delete sequences

```java

List<Item> deleteItemsInput = new ArrayList<>();

//By externalId
listSequencesResults.stream()
      .map(sequences -> Item.newBuilder()
              .setExternalId(sequences.getExternalId())
              .build())
      .forEach(item -> deleteItemsInput.add(item));

List<Item> deleteItemsResults = 
      client
      .sequences()
      .delete(deleteItemsInput);

//By id
listSequencesResults.stream()
      .map(sequences -> Item.newBuilder()
            .setId(sequences.getId())
            .build())
      .forEach(item -> deleteItemsInput.add(item));

List<Item> deleteItemsResults =
      client
      .sequences()
      .delete(deleteItemsInput);

```

### Insert rows

Insert rows into a sequence. If the row exists from before, then the specified columns will be overwritten. Columns that are not specified in the row object will retain their original value in CDF. 

If your row contains columns that are not a part of the sequence columns schema, then the SDK will try to update the sequence columns schema for you so the row can be inserted/updated. This is done on a best-effort basis.

```java

List<SequenceMetadata> upsertSequencesList = find();

List<SequenceBody> upsertSequenceBodyList = new ArrayList<>();
upsertSequencesList.forEach(sequence ->
        upsertSequenceBodyList.add(generateSequenceRows(sequence, 10)));
List<SequenceBody> upsertSequenceBodyResponse =
        client
        .sequences()
        .rows()
        .upsert(upsertSequenceBodyList);

//Example to generate data of SequenceBody
public static SequenceBody generateSequenceRows(SequenceMetadata header, int noRows) {
    List<SequenceColumn> columns = new ArrayList<>(header.getColumnsCount());
    List<SequenceRow> rows = new ArrayList<>(noRows);
    for (int i = 0; i < header.getColumnsCount(); i++) {
        columns.add(SequenceColumn.newBuilder()
        .setExternalId(header.getColumns(i).getExternalId())
        .build());
    }
    for (int i = 0; i < noRows; i++) {
        List<Value> values = new ArrayList<>(header.getColumnsCount());
        for (int j = 0; j < header.getColumnsCount(); j++) {
            if (ThreadLocalRandom.current().nextInt(1000) <= 2) {
              // Add a random null value for for 0.1% of the values.
              // Sequences support null values so we need to test for this
              values.add(Values.ofNull());
            } else if (header.getColumns(j).getValueType() == SequenceColumn.ValueType.DOUBLE) {
              values.add(Values.of(ThreadLocalRandom.current().nextDouble(1000000d)));
            } else if (header.getColumns(j).getValueType() == SequenceColumn.ValueType.LONG) {
              values.add(Values.of(ThreadLocalRandom.current().nextLong(10000000)));
            } else {
              values.add(Values.of(RandomStringUtils.randomAlphanumeric(5, 30)));
            }
        }
        rows.add(SequenceRow.newBuilder()
        .setRowNumber(i)
        .addAllValues(values)
        .build());
    }
    
    return SequenceBody.newBuilder()
        .setExternalId(header.getExternalId())
        .addAllColumns(columns)
        .addAllRows(rows)
        .build();
}
```

### Retrieve rows

```java

List<SequenceMetadata> listSequencesResults = find();

List<SequenceBody> listSequencesRowsResults = new ArrayList<>();
List<Item> sequenceBodyRequestItems = listSequencesResults.stream()
    .map(sequenceMetadata -> 
        Item.newBuilder().setId(sequenceMetadata.getId()).build())
    .collect(Collectors.toList());

client
    .sequences()
    .rows()
    .retrieveComplete(sequenceBodyRequestItems)
    .forEachRemaining(sequenceBodies -> listSequencesRowsResults.addAll(sequenceBodies));

```

Options filter:
- start:
  - integer <int64>
  - Default: 0
  - Lowest row number included.
- end:
  - integer <int64>
  - Get rows up to, but excluding, this row number. Default - No limit
- columns:
  - Array of strings [ 1 .. 200 ] items
  - Columns to be included. Specified as list of column externalIds. In case this filter is not set, all available columns will be returned.
- One of:
  - id:
    - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
    - A server-generated ID for the object.
  - externalId:
    - externalId string (CogniteExternalId) <= 255 characters
    - The external ID provided by the client. Must be unique for the resource type.

```java
List<SequenceBody> listSequencesRowsResults = new ArrayList<>();

Request request = Request.create
        .withRootParameter("id", 1L);

client
    .sequences()
    .rows()
    .retrieve(request)
    .forEachRemaining(sequenceBodies -> listSequencesRowsResults.addAll(sequenceBodies));


```

### Delete rows

```java
List<SequenceBody> deleteRowsInput = find();
List<SequenceBody> deleteRowsResults = 
        client
        .sequences()
        .rows()
        .delete(deleteRowsInput);
```