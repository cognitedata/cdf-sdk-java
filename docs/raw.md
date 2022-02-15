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

#### Create databases

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
    .delete("databaseName", List.of("tableName1", "tableName2"));

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

> **Note: The SDK will normally handle parallelization for you.**

#### List rows from a table

```java

List<RawRow> listRowsResults = new ArrayList<>();
client
    .raw()
    .rows()
    .list("databaseName", "tableName")
    .forEachRemaining(listRowsResults::addAll);

```

Options filter:
- minLastUpdatedTime:
  - integer <int64> (EpochTimestamp) >= 0
  - An exclusive filter, specified as the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
- maxLastUpdatedTime:
  - integer <int64> (EpochTimestamp) >= 0
  - An inclusive filter, specified as the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.

```java
long epochTime = new Date().getTime();
Request request = Request.create()
        .withRootParameter("minLastUpdatedTime", epochTime)
        .withRootParameter("maxLastUpdatedTime", epochTime);

List<RawRow> listRowsResults = new ArrayList<>();
client
    .raw()
    .rows()
    .list("databaseName", "tableName", request)
    .forEachRemaining(results -> results.stream().forEach(row -> listRowsResults.add(row)));

```

> **Note: Specialized list for distributed frameworks which offers custom control of the cursors.**

```java
long epochTime = new Date().getTime();
Request request = Request.create()
        .withRootParameter("minLastUpdatedTime", epochTime)
        .withRootParameter("maxLastUpdatedTime", epochTime);

List<RawRow> listRowsResults = new ArrayList<>();
client
    .raw()
    .rows()
    .list("databaseName", "tableName", request, "cursor")
    .forEachRemaining(results -> results.stream().forEach(row -> listRowsResults.add(row)));

```

- Change the `databaseName` to name of database
- Change the `tableName` to name of table
- Change the `cursor` to cursor value

#### Insert rows into a table

Insert rows into a table. It will replace the columns of an existing row if the rowKey already exists.

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
//First, we illustrate how to build a row using the DTO. This is similar to how you would build any other resource object.
public static List<RawRow> generateRawRows(String dbName, String tableName, int noObjects) {
List<RawRow> objects = new ArrayList<>();
    for (int i = 0; i < noObjects; i++) {
        RawRow row1 = RawRow.newBuilder()
                .setDbName(dbName)
                .setTableName(tableName)
                .setKey(RandomStringUtils.randomAlphanumeric(10))
                .setColumns(Struct.newBuilder()
                        .putFields("string", Values.of("my string value"))
                        .putFields("numeric", Values.of(1234))
                        .putFields("bool", Values.of(true))
                        .putFields("null_value", Values.ofNull())
                        .putFields("array", Values.of(ListValue.newBuilder()
                                .addValues(Values.of(1))
                                .addValues(Values.of(2))
                                .addValues(Values.of(3))
                                .build()))
                        .putFields("struct", Values.of(Structs.of(
                                "nestedString", Values.of("my nested string value")
                        )))
          ).build();
      objects.add(row1);
  }
  return objects;
}

// Build a row using the helper class util.RawRows. This allows you to define the row payload via a 
// plain Map<String, Object>        
// define the columns
Map<String, Object> myColumns = Map.of(
        "string-value", "my name value -",
        "int-value", 1234,
        "double-value", 23.5d,
        "nested-object", Map.of(
                "sub-name", "my sub-name value -",
                "sub-type", "a good sub-type"
        ),
        "num-list", List.of(1, 2, 3),
        "string-list", List.of("one", "two", "three")
);

// build a complete row
RawRow row2 = RawRows.of("dbName", "tableName", "rowKey", myColumns);

// Build a set of rows based on columns.
// First, build the basic key and column payloads
List<RawRow> allMyRows = new ArrayList<>();
for (<all the rows you need to iterate over>) {
    allMyRows.add(RawRows.of("rowKey", myColumns));
}

// Then add the db and table names
RawRows.setDbName(allMyRows, "myDbName");
RawRows.setTableName(allMyRows, "myTableName");
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