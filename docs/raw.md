## Raw

Manage data in the raw NoSQL database. Each project will have a variable number of raw databases, each of which will have a variable number of tables, each of which will have a variable number of key-value objects. Only queries on key are supported through this API.

PS: To create client see the file `clientSetup.md`

#### List databases

```java

List<String> listDatabaseResults = new ArrayList<>();
client
    .raw()
    .databases()
    .list()
    .forEachRemaining(databases -> listDatabaseResults.addAll(databases));

```

Options filter:
- limit:
    - integer [ 1 .. 1000 ]
    - Default: 25
    - Limit on the number of databases to be returned.
- cursor:
    - string
    - Example: cursor = 4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo
    - Cursor for paging through results.

```java
Request request = Request.create()
        .withRootParameter("cursor", "")
        .withRootParameter("limit", "");

List<String> listDatabaseResults = new ArrayList<>();
        client
        .raw()
        .databases()
        .list(request)
        .forEachRemaining(databases -> listDatabaseResults.addAll(databases));


```

#### Create databases

Create databases in a project. It is possible to post a maximum of 1000 databases per request.

```java

client
    .raw()
    .databases()
    .create(List.of("databaseName"));

```

PS:
- Change the `databaseName` to name of database

#### Delete databases

It deletes a database, but fails if the database is not empty and recursive is set to false (default).

```java

List<String> deleteItemsResults = 
        client
        .raw()
        .databases()
        .delete(List.of("databaseName"));

```

Options filter:
- recursive:
  - boolean
  - Default: false
  - When true, tables of this database are deleted with the database.

```java

List<String> deleteItemsResults = 
        client
        .raw()
        .databases()
        .delete(List.of("databaseName"), true);

```

PS:
- Change the `databaseName` to name of database

#### List tables in a database

```java

List<String> tablesResults = new ArrayList<>();
client
    .raw()
    .tables()
    .list("databaseName")
    .forEachRemaining(databases -> tablesResults.addAll(databases));

```

Options filter:
- limit:
  - integer [ 1 .. 1000 ]
  - Default: 25
  - Limit on the number of tables to be returned.
- cursor:
  - string
  - Example: cursor = 4zj0Vy2fo0NtNMb229mI9r1V3YG5NBL752kQz1cKtwo
  - Cursor for paging through results.

```java
Request request = Request.create()
        .withRootParameter("cursor", "")
        .withRootParameter("limit", "");

List<String> tablesResults = new ArrayList<>();
        client
        .raw()
        .tables()
        .list("databaseName", request)
        .forEachRemaining(databases -> tablesResults.addAll(databases));

```

PS:
- Change the `databaseName` to name of database

#### Create tables in a database

```java
Boolean ensureParent = false;
client
    .raw()
    .tables()
    .create("databaseName", List.of("tableName"), ensureParent);

```

Options of create:
- ensureParent:
  - boolean
  - Default: false
  - Create database if it doesn't exist already

PS:
- Change the `databaseName` to name of database
- Change the `tableName` to name of table

#### Delete tables in a database

```java
client
    .raw()
    .tables()
    .delete("databaseName", List.of("tableName"));

```

- Change the `databaseName` to name of database
- Change the `tableName` to name of table

#### Retrieve cursors for parallel reads

```java
client
    .raw()
    .tables()
    .delete("databaseName", List.of("tableName"));

```

- Change the `databaseName` to name of database
- Change the `tableName` to name of table

#### Retrieve cursors for parallel reads

Retrieve cursors based on the last updated time range. Normally this endpoint is used for reading in parallel.

Each cursor should be supplied as the 'cursor' query parameter on GET requests to Read Rows. Note that the 'minLastUpdatedTime' and the 'maxLastUpdatedTime' query parameter on Read Rows are ignored when a cursor is specified.

```java

List<RawRow> listRowsResults = new ArrayList<>();
client
    .raw()
    .rows()
    .list("databaseName", "tableName")
    .forEachRemaining(results -> results.stream().forEach(row -> listRowsResults.add(row)));

```

Options filter:
- minLastUpdatedTime:
  - integer <int64> (EpochTimestamp) >= 0
  - An exclusive filter, specified as the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- maxLastUpdatedTime:
  - integer <int64> (EpochTimestamp) >= 0
  - An inclusive filter, specified as the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- numberOfCursors:
  - integer <int32> [ 1 .. 10000 ]
  - The number of cursors to return, by default it's 10.

```java
long epochTime = new Date().getTime();
Request request = Request.create()
        .withRootParameter("minLastUpdatedTime", epochTime)
        .withRootParameter("maxLastUpdatedTime", epochTime)
        .withRootParameter("numberOfCursors", 10);

List<RawRow> listRowsResults = new ArrayList<>();
client
    .raw()
    .rows()
    .list("databaseName", "tableName", request)
    .forEachRemaining(results -> results.stream().forEach(row -> listRowsResults.add(row)));

```

- Change the `databaseName` to name of database
- Change the `tableName` to name of table

#### Insert rows into a table

Insert rows into a table. It is possible to post a maximum of 10000 rows per request. It will replace the columns of an existing row if the rowKey already exists.

The rowKey is limited to 1024 characters which also includes Unicode characters. The maximum size of columns are 5 MiB, however the maximum size of one column name and value is 2621440 characters each. If you want to store huge amount of data per row or column we recommend using the Files API to upload blobs, then reference it from the Raw row.

The columns object is a key value object, where the key corresponds to the column name while the value is the column value. It supports all the valid types of values in JSON, so number, string, array, and even nested JSON structure (see payload example to the right).

Note There is no rollback if an error occurs, which means partial data may be written. However, it's safe to retry the request, since this endpoint supports both update and insert (upsert).

```java

List<RawRow> createRowsList = 
        DataGenerator.generateRawRows("databaseName", "tableName", 10);

Boolean ensureParent = false;
List<RawRow> createRowsResults = 
        client
        .raw()
        .rows()
        .upsert(createRowsList, ensureParent);

//Example to generate data of RawRows
public static List<RawRow> generateRawRows(String dbName, String tableName, int noObjects) {
List<RawRow> objects = new ArrayList<>();
  for (int i = 0; i < noObjects; i++) {
      RawRow row1 = RawRow.newBuilder()
          .setDbName(dbName)
          .setTableName(tableName)
          .setKey(RandomStringUtils.randomAlphanumeric(10))
          .setColumns(Struct.newBuilder()
          .putFields("string", Value.newBuilder().setStringValue(RandomStringUtils.randomAlphanumeric(10)).build())
          .putFields("numeric", Value.newBuilder().setNumberValue(ThreadLocalRandom.current().nextDouble(10000d)).build())
          .putFields("bool", Value.newBuilder().setBoolValue(ThreadLocalRandom.current().nextBoolean()).build())
          .putFields("null_value", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
          .putFields("array", Value.newBuilder().setListValue(ListValue.newBuilder()
          .addValues(Value.newBuilder().setNumberValue(ThreadLocalRandom.current().nextDouble(10000d)).build())
          .addValues(Value.newBuilder().setNumberValue(ThreadLocalRandom.current().nextDouble(10000d)).build())
          .addValues(Value.newBuilder().setNumberValue(ThreadLocalRandom.current().nextDouble(10000d)).build())
          .build()).build())
          .putFields("struct", Value.newBuilder().setStructValue(Struct.newBuilder()
          .putFields("nestedString", Value.newBuilder().setStringValue("myTrickyStringValue_æøå_äö")
          .build())).build())
          ).build();
      objects.add(row1);
  }
  return objects;
}

```

Options of create:
- ensureParent:
  - boolean
  - Default: false
  - Create database/table if it doesn't exist already

#### Retrieve row by key

```java

List<RawRow> rowsRetrieved = 
        client
        .raw()
        .rows()
        .retrieve("databaseName", "tableName", List.of("rowKeys"));

```

- Change the `databaseName` to name of database
- Change the `tableName` to name of table
- Change the `rowKeys` to row keys

#### Delete rows in a table

```java

List<RawRow> rowsToDelete = findRow();
List<RawRow> deleteRowResults = 
        client
        .raw()
        .rows()
        .delete(rowsToDelete);

```