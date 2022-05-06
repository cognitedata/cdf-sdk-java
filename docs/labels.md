
# Labels

### Create label definitions.

Creates label definitions that can be used across different resource types. The label definitions are uniquely identified by their external id.

```java
List<Label> upsertLabelsList =  List.of(Label.newBuilder() 
          .setExternalId("10") 
          .setName("Label name") 
          .setDescription("Description") 
          .build()); 

List<Label>  list = client.labels().upsert(upsertLabelsList); 
```

### Filter labels

Creates label definitions that can be used across different resource types. The label definitions are uniquely identified by their external id.

```java
List<Label> listLabelsResults = new ArrayList<>(); 
client.labels() 
          .list() 
          .forEachRemaining(labels -> listLabelsResults.addAll(labels)); 

client.labels() 
          .list(Request.create() 
               .withFilterParameter("externalIdPrefix", "Val")) 
          .forEachRemaining(labels -> listLabelsResults.addAll(labels)); 
```

Options filter:
- filter:
    - name:
        - string [ 1 .. 140 ] characters
        - Returns the label definitions matching that name.
    - externalIdPrefix:
        - string (ExternalIdPrefixFilter) <= 255 characters
        - filter external ids starting with the prefix specified
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

### Delete label definitions.

Delete all the label definitions specified by their external ids. The resource items that have the corresponding label attached remain unmodified. It is up to the client to clean up the resource items from their attached labels if necessary.

```java
List<Item> deleteItemsByExternalIds = List.of(Item.newBuilder() 
          .setExternalId("10") 
          .build()); 
List<Item> deleteItemsResults = 
          client.labels().delete(deleteItemsByExternalIds); 

List<Item> deleteItemsByInternalIds = List.of(Item.newBuilder() 
          .setId(10) 
          .build()); 
List<Item> deleteItemsResults = 
          client.labels().delete(deleteItemsByInternalIds);  
```