## Contextualization

The contextualization services of Cognite Data Fusion helps enrich and connect data. The services offer
different algorithms for information extraction/feature generation and matching. The SDK supports 
the following contextualization services:
- Entity matching.
- Interactive engineering diagrams (P&IDs).

Example of an interactive P&ID pipeline: https://github.com/cognitedata/cdf-sdk-java-examples/tree/main/interactive-pnid-batch-job

### Entity matching

TBD

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