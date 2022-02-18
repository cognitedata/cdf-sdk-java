## Contextualization

The contextualization services of Cognite Data Fusion helps enrich and connect data. The services offer
different algorithms for information extraction/feature generation and matching. The SDK supports 
the following contextualization services:
- Entity matching.
- Interactive engineering diagrams (P&IDs).

Example of an interactive P&ID pipeline: https://github.com/cognitedata/cdf-sdk-java-examples/tree/main/interactive-pnid-batch-job

> Note: To create client see the file [clientSetup.md](clientSetup.md)

### Entity matching

#### Create entity matcher model

> Note: All users on this CDF subscription with assets read-all and entitymatching read-all and write-all capabilities in the project, are able to access the data sent to this API. Train a model that predicts matches between entities (for example, time series names to asset names). This is also known as fuzzy joining. If there are no trueMatches (labeled data), you train a static (unsupervised) model, otherwise a machine learned (supervised) model is trained.

```java

ImmutableList<Struct> source = generateSourceStructs();
ImmutableList<Struct> target = generateTargetTrainingStructs();

Request entityMatchFitRequest = Request.create()
        .withRootParameter("sources",  source)
        .withRootParameter("targets", target)
        .withRootParameter("matchFields", ImmutableList.of(
                    ImmutableMap.of("source", "name", "target", "externalId")
                ))
        .withRootParameter("featureType", featureType);

List<EntityMatchModel> models = 
        client
        .contextualization()
        .entityMatching()
        .create(ImmutableList.of(entityMatchFitRequest));

//Example to generate data of Struct
private ImmutableList<Struct> generateSourceStructs() {
    Struct entityA = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(1D).build())
        .putFields("name", Value.newBuilder().setStringValue("23-DB-9101").build())
        .putFields("fooField", Value.newBuilder().setStringValue("bar").build())
        .build();
    Struct entityB = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(2D).build())
        .putFields("name", Value.newBuilder().setStringValue("23-PC-9101").build())
        .putFields("barField", Value.newBuilder().setStringValue("foo").build())
        .build();
    Struct entityC = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(3D).build())
        .putFields("name", Value.newBuilder().setStringValue("343-Å").build())
        .build();
    return ImmutableList.of(entityA, entityB, entityC);
}

private ImmutableList<Struct> generateTargetTrainingStructs() {
    Struct targetA = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(1D).build())
        .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
        .build();
    Struct targetB = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(2D).build())
        .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
        .build();
    return ImmutableList.of(targetA, targetB);
}

```

#### Predict matches

> Note: All users on this CDF subscription with assets read-all and entitymatching read-all and write-all capabilities in the project, are able to access the data sent to this API. Predicts entity matches using a trained model.

```java
EntityMatchModel entityMatchModel = find();
ImmutableList<Struct> source = generateSourceStructs();
ImmutableList<Struct> target = generateTargetStructs();

//By id
List<EntityMatchResult> matchResultsById = 
        client
        .contextualization()
        .entityMatching()
        .predict(entityMatchModel.getId(), source, target);

//By externalId
List<EntityMatchResult> matchResultsByExternalId =
        client
        .contextualization()
        .entityMatching()
        .predict(entityMatchModel.getExternalId(), source, target);

//With numMatches and scoreThreshold
//numMatches - The maximum number of match candidates per source.
//scoreThreshold - The minimum score required for a match candidate.
int numMatches = 1;
double scoreThreshold = 1D;

List<EntityMatchResult> matchResultsByExternalId =
        client
        .contextualization()
        .entityMatching()
        .predict(entityMatchModel.getExternalId(), source, target, numMatches);

List<EntityMatchResult> matchResultsByExternalId =
        client
        .contextualization()
        .entityMatching()
        .predict(entityMatchModel.getExternalId(), source, target, numMatches, scoreThreshold);

//Example to generate data of Struct
private ImmutableList<Struct> generateSourceStructs() {
    Struct entityA = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(1D).build())
        .putFields("name", Value.newBuilder().setStringValue("23-DB-9101").build())
        .putFields("fooField", Value.newBuilder().setStringValue("bar").build())
        .build();
    Struct entityB = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(2D).build())
        .putFields("name", Value.newBuilder().setStringValue("23-PC-9101").build())
        .putFields("barField", Value.newBuilder().setStringValue("foo").build())
        .build();
    Struct entityC = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(3D).build())
        .putFields("name", Value.newBuilder().setStringValue("343-Å").build())
        .build();
    return ImmutableList.of(entityA, entityB, entityC);
}

private ImmutableList<Struct> generateTargetStructs() {
    Struct targetA = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(1D).build())
        .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
        .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
        .build();
    Struct targetB = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setNumberValue(2D).build())
        .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
        .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
        .build();
    return ImmutableList.of(targetA, targetB);
}

```

#### Delete entity matcher model

Deletes an entity matching model. Currently, this is a soft delete, and only removes the entry from listing.

```java
EntityMatchModel entityMatchModel = find();

//By id
Item modelItemById = Item.newBuilder()
        .setId(entityMatchModel.getId())
        .build();
List<Item> deleteResults = 
        client
        .contextualization()
        .entityMatching()
        .delete(ImmutableList.of(modelItemById));

//By externalId
Item modelItemByExternalId = Item.newBuilder()
        .setId(entityMatchModel.getExternalId())
        .build();
List<Item> deleteResults =
        client
        .contextualization()
        .entityMatching()
        .delete(ImmutableList.of(modelItemByExternalId));

```

### Interactive engineering diagrams (P&IDs)
The engineering diagrams service performs two main tasks:
1. Detect entities and annotate them.
2. Convert PDF/image diagrams to an "interactive" SVG version.

Entities/annotations can be detected via text matching. You specify a set of `entities`to use as the basis for the matching. 
An entity can be specified as follows:
```java
Struct entity = Struct.newBuilder()
        .putFields("name", Values.of("1N914"))
        .putFields("resourceType", Values.of("Asset"))
        .putFields("externalId", Values.of("my-external-id-1"))
        .putFields("id", Values.of(146379580567867L))
        .build();
```
In the default configuration, the annotations service will scan the diagram document for the text in the `name` attribute--
and if found it will build an annotation containing the other entity attributes as metadata.

The list of entities and diagram (P&ID) files are passed as arguments to the detect annotations service. Here you also specify
whether to build an interactive SVG version of the file or not:
```java
// Detecting annotations based on exact text matching
List<DiagramResponse> detectResults = client.experimental()
        .pnid()
        .detectAnnotationsPnID(fileItems,       // The P&IDs to process--represented as a List<Item>
                                entities,       // The List<Struct> of entities to try and match 
                                "name",         // The entity attribute to use as search text
                                true);          // If set to 'true', will generate an interactive SVG

// If you want to use fuzzy matching...
        List<DiagramResponse> detectResults = client.experimental()
        .engineeringDiagrams()
        .detectAnnotations(fileItems,       // The P&IDs to process--represented as a List<Item>
                                entities,       // The List<Struct> of entities to try and match 
                                "name",         // The entity attribute to use as search text
                                true,           // Enable partial matching
                                2,              // The minimum number of tokens (consecutive letters/numbers) required for a match.
                                true);          // If set to 'true', will generate an interactive SVG

// Collecting the interactive SVG/PNG from the response. 
for (DiagramResponse response : detectResults) {
    // The original file may have multiple pages, so we have to loop through the results.
    for (DiagramResponse.ConvertResult result : response.getConvertResultsList()) {
        int pageNo = result.getPage();          // 
        if (result.hasSvgBinary()) {
            ByteArray svgBytes = response.getSvgBinary().toByteArray();
            // do something smart with the bytes...
        }
        if (result.hasPngBinary()) {
            ByteArray pngBytes = response.getPngBinary().toByteArray();
            // do something smart with the bytes...
        }
    }
}
```