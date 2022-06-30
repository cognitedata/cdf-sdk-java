# Transformations

Transformations enable users to use Spark SQL queries to transform data from the CDF staging area, RAW, into the CDF data model.

### Create transformations

```java

private static final Integer COUNT_TO_BE_CREATE_TD = 1;
Long dataSetId = getOrCreateDataSet(client);

List<Transformation> listToBeCreate = new ArrayList<>();

List<Transformation> generatedWithDestinationDataSource1List = 
        DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, "data_source", 2,
            TestConfigProvider.getClientId(),
            TestConfigProvider.getClientSecret(),
            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
            TestConfigProvider.getProject());

List<Transformation> generatedWithDestinationRawDataSourceList = 
        DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, "raw", 2,
            TestConfigProvider.getClientId(),
            TestConfigProvider.getClientSecret(),
            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
            TestConfigProvider.getProject());

listToBeCreate.addAll(generatedWithDestinationDataSource1List);
listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);

List<Transformation> createdList = client.transformation().upsert(listToBeCreate);

//Examples to generate data of Transformation
private Long getOrCreateDataSet(CogniteClient client) throws Exception {
    List<DataSet> upsertDataSetList = DataGenerator.generateDataSets(1);
    List<DataSet> upsertDataSetsResults = client.datasets().upsert(upsertDataSetList);
    Long dataSetId = upsertDataSetsResults.get(0).getId();    
    return dataSetId;
}

public static List<DataSet> generateDataSets(int noObjects) {
    List<DataSet> objects = new ArrayList<>();
    for (int i = 0; i < noObjects; i++) {
        objects.add(DataSet.newBuilder()
            .setExternalId(RandomStringUtils.randomAlphanumeric(10))
            .setName("generated-" + RandomStringUtils.randomAlphanumeric(5))
            .setDescription("Generated description")
            .putMetadata("type", "sdk-data-generator")
            .putMetadata("source", "sdk-data-generator")
            .build());
    }
    return objects;
}

/**
 *
 * @param noObjects
 * @param dataSetId
 * @param destinationType 1 = DataSource1, 2 = RawDataSource and 3 = SequenceRowDataSource
 * @param typeOfCredentials 1 = ApiKey and 2 = OidcCredentials
 * @param clientId
 * @param clientSecret
 * @param tokenUri
 * @param cdfProjectName
 * @return
 */
public static List<Transformation> generateTransformations(Integer noObjects, long dataSetId, String destinationType, int typeOfCredentials, String clientId, String clientSecret, String tokenUri, String cdfProjectName) {
    List<Transformation> objects = new ArrayList<>(noObjects);
    for (int i = 0; i < noObjects; i++) {
        Transformation.Builder builder = Transformation.newBuilder()
            .setName("TransformationTestSDK-"+RandomStringUtils.randomAlphanumeric(10))
            .setQuery("select * from teste")
            .setConflictMode("upsert")
            .setIsPublic(true)
            .setExternalId("TestSKD-"+RandomStringUtils.randomAlphanumeric(10))
            .setIgnoreNullFields(true)
            .setDataSetId(dataSetId);
        
        if ("raw".equals(destinationType)) {
            builder.setDestination(Transformation.Destination.newBuilder()
            .setType("raw")
            .setDatabase("Test")
            .setTable("Test")
            .build());
        } else if ("sequence_rows".equals(destinationType)) {
            builder.setDestination(Transformation.Destination.newBuilder()
            .setType("sequence_rows")
            .setExternalId("Test-sequence_rows")
            .build());
        } else {
            builder.setDestination(Transformation.Destination.newBuilder()
            .setType(Transformation.Destination.DataSourceType.ASSETS.toString())
            .build());
        }
        
        if (typeOfCredentials == 1) {
            builder.setSourceApiKey("TesteApiKey");
            builder.setDestinationApiKey("TesteApiKey");
        } else if(typeOfCredentials == 2) {
            builder.setSourceOidcCredentials(Transformation.FlatOidcCredentials.newBuilder()
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setScopes("https://greenfield.cognitedata.com/.default")
                .setTokenUri(tokenUri.toString())
                .setCdfProjectName(cdfProjectName)
                .setAudience("")
                .build());
            builder.setDestinationOidcCredentials(Transformation.FlatOidcCredentials.newBuilder()
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setScopes("https://greenfield.cognitedata.com/.default")
                .setTokenUri(tokenUri.toString())
                .setCdfProjectName(cdfProjectName)
                .setAudience("")
                .build());
        }
        objects.add(builder.build());
    }
    return objects;
}

```

### Retrieve transformations

```java

List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
List<Transformation> retrievedTransformation = client.transformation().retrieve(items);

```

Options filter:
- items:
    - id
        - The external ID provided by the client. Must be unique for the resource type.
        - Array of integers `int64` [ 0 .. 100 ] items [ items &lt;int64&gt; ]
    - externalId
        - A server-generated ID for the object.
        - Array of integers `int64` [ 0 .. 100 ] items [ items &lt;int64&gt; ]
- ignoreUnknownIds:
    - boolean
    - Default: false
    - Ignore IDs and external IDs that are not found
- withJobDetails:
    - boolean
    - Whether the transformations will be returned with running job and last created job details.

### Filter transformations

```java

List<Transformation> listResults = new ArrayList<>();
     client.transformation()
             .list(Request.create()
                         .withFilterParameter("isPublic", true))
             .forEachRemaining(listResults::addAll);

```

Options filter:
- filter:
    - isPublic:
        - boolean
        - Whether public transformations should be included in the results. Defaults to `true`.
    - nameRegex:
       - string
       - Regex expression to match the transformation name.
    - queryRegex:
        - string
        - Regex expression to match the transformation query.
    - destinationType:
        - string
        - Transformation destination resource name to filter by.
    - conflictMode:
        - string
        - Filters by a selected transformation action type: `abort`, `upsert`, `update`, `delete`.
    - hasBlockedError:
        - boolean
        - Whether only the blocked transformations should be included in the results.
    - cdfProjectName:
        - string
        - Project name to filter by configured source and destination project.
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
        - Array of CogniteExternalId (string) or CogniteInternalId (integer)[ items ]
        - Return only transformations in the specified data sets.
        - One of:
            - CogniteInternalId:
                - id integer <int64> (CogniteInternalId) [ 1 .. 9007199254740991 ]
                - A server-generated ID for the object.
            - CogniteExternalId:
                - externalId string (CogniteExternalId) <= 255 characters
                - The external ID provided by the client. Must be unique for the resource type.

### Delete transformations

```java

List<Item> deleteItemsInput = List.of(Item.newBuilder().setExternalId("1").build());
List<Item> deletedItemsResults = client.transformation().delete(deleteItemsInput);

```

Options filter:
- ignoreUnknownIds
  - boolean
  - Default: false
  - Ignore IDs and external IDs that are not found
  
### Update transformations

Update the attributes of transformations

```java

client.transformation().upsert(listToBeUpdate);

```

### Run a transformation

```java
Long transformationId = 1L;
Transformation.Job job =
        client.transformation().jobs().run(transformationId);

```

### Cancel a transformation

```java
Long transformationId = 1L;
Boolean jobResult =
        client.transformation().jobs().cancel(transformationId);

```


# Transformation Jobs

Transformation jobs let you execute transformations asynchronously.

### List jobs

```java
List<Transformation.Job> listResults = new ArrayList<>();
client.transformation()
        .jobs()
        .list()
        .forEachRemaining(listResults::addAll);
```

Options filter:
- transformationId
    - integer
    - List only jobs for the specified transformation. The transformation is identified by ID.
- transformationExternalId
    - string
    - List only jobs for the specified transformation. The transformation is identified by external ID.


### Retrieve jobs by ids

```java
List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
List<Transformation.Job> retrievedTransformationJobs = client.transformation().jobs().retrieve(items);
```

Options filter:
- ignoreUnknownIds
    - boolean
    - Ignore IDs and external IDs that are not found. Defaults to false.


### List job metrics by job id

```java
Integer jobId = 1;
List<Transformation.Job.Metric> listResults = new ArrayList<>();
client.transformation()
        .jobs()
        .metrics()
        .list(jobId)
        .forEachRemaining(listResults::addAll);
```

# Transformation Notifications

Transformation notifications let users know when a job fails if subscribed.

### List notification subscriptions

```java

List<Transformation.Notification> listResults = new ArrayList<>();
client.transformation()
      .notifications()
      .list()
      .forEachRemaining(listResults::addAll);

```

Options filter:
- transformationId
    - integer
    - List only jobs for the specified transformation. The transformation is identified by ID.
- transformationExternalId
    - string
    - List only jobs for the specified transformation. The transformation is identified by external ID.
- destination
    - string
    - Filter by notification destination.


### Subscribe for notifications

Subscribe for notifications on the transformation errors.

```java

List<Transformation.Notification.Subscription> subscribes = new ArrayList<>();

subscribes.add(Transformation.Notification.Subscription.newBuilder()
    .setDestination("pires@test.com")
    .setTransformationId(1L)
    .build());

List<Transformation.Notification> createdSubscribes =
        client.transformation()
              .notifications()
              .subscribe(subscribes);

```

Options filter:
- items:
  - One of:
      - transformationExternalId
          - string
          - Transformation external ID to subscribe.
      - transformationId
          - integer
          - Transformation ID to subscribe.
  - destination
      - string
      - Email address where notifications should be sent.


### Delete notification subscriptions by notification ID

Deletes the specified notification subscriptions on the transformation.

```java

List<Item> items = List.of(Item.newBuilder().setId(1).build());
List<Item> deletedItemsResults = 
                client.transformation()
                      .notifications()
                      .delete(items);

```

# Transformation Schedules

Transformation schedules allow you to run transformations with a specific input at intervals defined by a cron expression. These transformation jobs will be asynchronous and show up in the transformation job list. Visit http://www.cronmaker.com to generate a cron expression with a UI.

### List all schedules

List all transformation schedules.

```java
List<Transformation.Schedule> listResults = new ArrayList<>();
client.transformation()
      .schedules()
      .list()
      .forEachRemaining(listResults::addAll);
```

Options filter:
- items:
    - includePublic
        - boolean
        - Whether public transformations should be included in the results. The default is true.


### Schedule transformations

Schedule transformations with the specified configurations.

```java
private static final Integer COUNT_TO_BE_CREATE_TD = 1;
private String transformationExternalId = 1L;

List<Transformation.Schedule> schedules =
        DataGenerator.generateTransformationSchedules(COUNT_TO_BE_CREATE_TD, transformationExternalId, "*/5 * * * *", false);

List<Transformation.Schedule> createdListSchedules = 
        client.transformation()
              .schedules()
              .schedule(schedules);

//Examples to generate data of Schedule
public static List<Transformation.Schedule> generateTransformationSchedules(Integer noObjects, String transformationExternalId, String interval, Boolean isPaused) {
    List<Transformation.Schedule> objects = new ArrayList<>(noObjects);
    for (int i = 0; i < noObjects; i++) {
        objects.add(Transformation.Schedule.newBuilder()
        .setExternalId(transformationExternalId)
        .setInterval(interval)
        .setIsPaused(isPaused)
        .build());
    }
    return objects;
}
```

### Retrieve schedules

Retrieve transformation schedules by transformation IDs or external IDs.

```java
List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
List<Transformation> retrievedSchedules = 
        client.transformation()
              .schedules()
              .retrieve(items);
```

Options filter:
- items:
    - Array of CogniteExtId (object) or CogniteIntId (object) (CogniteId) [ items ]
    - One of:
        - CogniteExtId
            - string
            - The external ID provided by the client. Must be unique for the resource type.
        - CogniteIntId
            - integer
            - A server-generated ID for the object.
    - ignoreUnknownIds:
      - boolean
      - Ignore IDs and external IDs that are not found. Defaults to false.


### Unschedule transformations

Unschedule transformations by IDs or externalIds.

```java
List<Item> items = List.of(Item.newBuilder().setId(1).build());
client.transformation()
        .schedules()
        .unSchedule(items);
```

Options filter:
- items:
    - Array of CogniteExtId (object) or CogniteIntId (object) (CogniteId) [ items ]
    - One of:
        - CogniteExtId
            - string
            - The external ID provided by the client. Must be unique for the resource type.
        - CogniteIntId
            - integer
            - A server-generated ID for the object.


### Update schedules

```java
private static final Integer COUNT_TO_BE_CREATE_TD = 1;
private String transformationExternalId = 1L;

List<Transformation.Schedule> listNewSchedules =
DataGenerator.generateTransformationSchedules(COUNT_TO_BE_CREATE_TD, transformationExternalId, "*/5 * * * *", false);

List<Transformation.Schedule> createdListSchedules =
        client.transformation().schedules().schedule(listNewSchedules);
assertEquals(listNewSchedules.size(), createdListSchedules.size());

List<Transformation.Schedule> editedInput = createdListSchedules.stream()
        .map(sche -> sche.toBuilder()
                .setIsPaused(true)
                .setInterval("*/10 * * * *")
                .build())
        .collect(Collectors.toList());

List<Transformation.Schedule> updatedList =
        client.transformation().schedules().schedule(editedInput);
```
