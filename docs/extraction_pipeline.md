## Extraction Pipelines

Extraction Pipeline objects represent the applications and software that are deployed to ingest operational data into CDF. An extraction pipeline can consist of a number of different software components between the source system and CDF. The extraction pipeline object represents the software component that actually sends the data to CDF. Two examples are Cognite extractors and third party ETL tools such as Microsoft Azure or Informatica PowerCenter

> Note: To create client see the file [clientSetup.md](clientSetup.md)

### List extraction pipelines

Returns a list of all extraction pipelines for a given project

```java
List<ExtractionPipeline> listPipelinesResults = 
        new ArrayList<>();
client
    .extractionPipelines()
    .list()
    .forEachRemaining(listPipelinesResults::addAll);
```

### Filter extraction pipelines

Use advanced filtering options to find extraction pipelines.

```java
List<ExtractionPipeline> listPipelinesResults = 
        new ArrayList<>();

Request request = Request.create()
        .withFilterParameter("name", "name");

client
    .extractionPipelines()
    .list(request)
    .forEachRemaining(listPipelinesResults::addAll);
```

Options filter:
- externalId:
    - string [ 1 .. 255 ] characters
    - External Id provided by client. Should be unique within the project.
- name:
    - string [ 1 .. 140 ] characters
    - Name of Extraction Pipeline
- description:
    - string <= 500 characters
    - Description of Extraction Pipeline
- source:
    - string or null <= 255 characters
    - Source for Extraction Pipeline
- documentation:
    - string <= 10000 characters
    - Documentation text field, supports Markdown for text formatting.
- id:
    - integer <int64> [ 0 .. 9007199254740991 ]
    - A server-generated ID for the object.
- lastSuccess:
    - integer <int64>
    - Time of last successful run. The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- lastFailure:
    - integer <int64>
    - Time of last failure run. The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- lastMessage:
    - string
    - Last failure message.
- lastSeen:
    - integer <int64>
    - Last seen time. The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- createdTime:
    - integer <int64> (EpochTimestamp) >= 0
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- lastUpdatedTime:
    - integer <int64> (EpochTimestamp) >= 0
    - The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- createdBy:
    - string
    - Extraction Pipeline creator. Usually user email is expected here
- schedule:
  - Possible values: “On trigger”, “Continuous” or cron expression. If empty then “Null“.
- rawTables:
    - Array of objects
    - RawTables Object
```
EX: "rawTables": [
      {
        "dbName": "string",
        "tableName": "string"
      }
  ]
```
- contacts:
    - Contacts Object
    - Contacts list.
```
EX: "contacts": [
        {
          "name": "string",
          "email": "user@example.com",
          "role": "string",
          "sendNotification": true
        }
      ]
```
- metadata:
  - Object
  - Custom, application specific metadata. String key -> String value. Limits: Key are at most 128 bytes. Values are at most 10240 bytes. Up to 256 key-value pairs. Total size is at most 10240.
```
EX: "metadata": {
      "property1": "string",
      "property2": "string"
    }
```

### Retrieve extraction pipelines

Retrieves information about multiple extraction pipelines in the same project.

```java
List<ExtractionPipeline> listByIds = 
        client
            .extractionPipelines()
            .retrieve(1, 2);

List<ExtractionPipeline> listByExternalIds = 
        client
        .extractionPipelines()
        .retrieve("1", "2");

List<ExtractionPipeline> list = 
        client
        .extractionPipelines()
        .retrieve(List.of(Item.newBuilder()
            .setId(1)
            .build())
        );
```

### Create extraction pipelines

Creates multiple new extraction pipelines.

```java

long dataSetId = find();

List<ExtractionPipeline> upsertPipelinesList = 
            generateExtractionPipelines(3, dataSetId);
client
    .extractionPipelines()
    .upsert(upsertPipelinesList);

//Example to generate data of ExtractionPipeline
public static List<ExtractionPipeline> generateExtractionPipelines(int noObjects, long dataSetId) {
List<ExtractionPipeline> objects = new ArrayList<>();
    for (int i = 0; i < noObjects; i++) {
        objects.add(ExtractionPipeline.newBuilder()
            .setExternalId(RandomStringUtils.randomAlphanumeric(10))
            .setName("generated-" + RandomStringUtils.randomAlphanumeric(5))
            .setDescription("Generated description")
            .setDataSetId(dataSetId)
            .setSource(sourceValue)
            .putMetadata("type", DataGenerator.sourceValue)
            .putMetadata(sourceKey, DataGenerator.sourceValue)
            .addContacts(ExtractionPipeline.Contact.newBuilder()
            .setName("generated-" + RandomStringUtils.randomAlphanumeric(5))
            .setRole("generated")
            .build())
        .build());
    }
return objects;
}

```

### Update extraction pipelines

Update information for a list of extraction pipelines.

```java

List<ExtractionPipeline> upsertPipelinesList = findList();
client
    .extractionPipelines()
    .upsert(upsertPipelinesList);


```

> Note: To use replace mode

```java
client.getClientConfig().withUpsertMode(UpsertMode.REPLACE);
List<ExtractionPipeline> upsertPipelinesList = findList();
client
    .extractionPipelines()
    .upsert(upsertPipelinesList);
```

### Delete extraction pipelines

Delete extraction pipelines for given list of ids and externalIds. When the extraction pipeline is deleted, all extraction pipeline runs related to the extraction pipeline are automatically deleted.

```java

List<Item> deleteItemsInputByExternalId = listPipelinesResults.stream()
                    .map(pipeline -> Item.newBuilder()
                            .setExternalId(pipeline.getExternalId())
                            .build())
                    .collect(Collectors.toList());

List<Item> deleteItemsResults = 
        client
        .extractionPipelines()
        .delete(deleteItemsInputByExternalId);


List<Item> deleteItemsInputByInternalId = listPipelinesResults.stream()
                    .map(pipeline -> Item.newBuilder()
                          .setId(pipeline.getId())
                          .build())
                    .collect(Collectors.toList());

List<Item> deleteItemsResults =
        client
        .extractionPipelines()
        .delete(deleteItemsInputByInternalId);

```


## Extraction Pipelines Runs

Extraction Pipelines Runs are CDF objects to store statuses related to an extraction pipeline. The supported statuses are: success, failure and seen. The statuses are related to two different types of operation of the extraction pipeline. Success and failure indicate the status for a particular EP run where the EP attempts to send data to CDF. If the data is successfully posted to CDF the status of the run is ‘success’; if the run has been unsuccessful and the data is not posted to CDF, the status of the run is ‘failure’. Message can be stored to explain run status. Seen is a heartbeat status that indicates that the extraction pipeline is alive. This message is sent periodically on a schedule and indicates that the extraction pipeline is working even though data may not have been sent to CDF by the extraction pipeline.

### List extraction pipeline runs

List of all extraction pipeline runs for a given extraction pipeline.

```java

List<ExtractionPipelineRun> listPipelinesRunResults = new ArrayList<>();
client
    .extractionPipelines()
    .runs()
    .list()
    .forEachRemaining(listPipelinesRunResults::addAll);

```

### Filter extraction pipeline runs

Use advanced filtering options to find extraction pipeline runs

```java

Request reques = Request.create()
                .withFilterParameter("externalId", "externalId")
                .withFilterParameter("statuses", List.of("success", "failure", "seen"));
client
    .extractionPipelines()
    .runs()
    .list(reques)
    .forEachRemaining(listPipelinesRunResults::addAll);

```

Options filter:
- externalId:
  - string [ 1 .. 255 ] characters
  - Extraction pipeline external Id provided by client.
- statuses:
  - Array of strings (ExtPipeRunStatus)
  - Items Enum: `success` `failure` `seen`
  - Extraction pipeline statuses list. Expected values: success, failure, seen.
- createdTime:
  - object (EpochTimestampRange)
  - Range between two timestamps (inclusive).
    - max:
      - integer <int64> >= 0
      - Maximum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    - min:
      - integer <int64> >= 0
      - Minimum timestamp (inclusive). The timestamp is represented as number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
```
EX: "createdTime": {
      "max": 0,
      "min": 0
    }
```
- message:
  - object (StringFilter)
```
EX: "message": {
        "substring": "string"
    }
```

### Create extraction pipeline runs

Create multiple extraction pipeline runs. Extraction pipeline runs support three statuses: success, failure, seen.

```java

List<ExtractionPipelineRun> upsertPipelineRunsList = 
        generateExtractionPipelineRuns(3, extractionPipelineId);

List<ExtractionPipelineRun> list = client
        .extractionPipelines()
        .runs()
        .create(upsertPipelineRunsList);

//Example to generate data of ExtractionPipelineRun
public static List<ExtractionPipelineRun> generateExtractionPipelineRuns(int noObjects, String pipelineExtId) {
  List<ExtractionPipelineRun> objects = new ArrayList<>();
      for (int i = 0; i < noObjects; i++) {
          objects.add(ExtractionPipelineRun.newBuilder()
            .setExternalId(pipelineExtId)
            .setCreatedTime(Instant.now().toEpochMilli())
            .setMessage("generated-" + RandomStringUtils.randomAlphanumeric(5))
            .setStatus(ExtractionPipelineRun.Status.SUCCESS)
            .build());
      }
      return objects;
  }
```