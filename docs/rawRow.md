## Raw Rows

The `raw` resource type is a very flexible data container resembling a [wide-column store](https://en.wikipedia.org/wiki/Wide-column_store). 
This flexibility also means that you have to deal with a wide variety of data schemas and parsing. Below, we'll try and 
illustrate how to work with `RawRow` objects.

### Create / Build Raw Rows

`RawRow` can be created using the native DTO or via the helper class `com.cognite.client.util.RawRows`.
```java
// Build a row using the DTO. This is similar to how you would build any other resource object.
RawRow row1 = RawRow.newBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setKey("the-row-key")
        .setColumns(Struct.newBuilder()
                .putFields("string", Values.of("my string value"))
                .putFields("numeric", Values.of(1234)
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

### Parse Raw Rows

When working with data staged in `Raw`, you often need to parse `RawRow` objects into other data structures, such as 
`events` or `assets` or other.
