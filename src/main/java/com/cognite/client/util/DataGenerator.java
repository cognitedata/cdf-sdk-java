package com.cognite.client.util;

import com.cognite.client.dto.*;
import com.google.protobuf.*;
import com.google.protobuf.util.Values;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for generating random data objects.
 */
public class DataGenerator {
    public static final String sourceKey = "source";
    public static final String sourceValue = "sdk-data-generator";

    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60L * SECOND_MS;
    private static final long HOUR_MS = 60L * MINUTE_MS;
    private static final long DAY_MS = 24L * HOUR_MS;

    public static List<FileMetadata> generateFileHeaderObjects(int noObjects) {
        List<FileMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(FileMetadata.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("test_file_" + RandomStringUtils.randomAlphanumeric(5) + ".test")
                    .setSource(sourceValue)
                    .putMetadata("type", DataGenerator.sourceValue)
                    .putMetadata(sourceKey, DataGenerator.sourceValue)
                    .build());
        }
        return objects;
    }

    public static List<TimeseriesMetadata> generateTsHeaderObjects(int noObjects) {
        List<TimeseriesMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(TimeseriesMetadata.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("test_ts_" + RandomStringUtils.randomAlphanumeric(5))
                    .setIsString(false)
                    .setIsStep(false)
                    .setDescription(RandomStringUtils.randomAlphanumeric(50))
                    .setUnit("TestUnits")
                    .putMetadata("type", DataGenerator.sourceValue)
                    .putMetadata(sourceKey, DataGenerator.sourceValue)
                    .build());
        }
        return objects;
    }

    public static List<TimeseriesPointPost> generateTsDatapointsObjects(int noItems, double frequency,
                                                                        List<String> externalIdList) {
        List<TimeseriesPointPost> items = new ArrayList<>(noItems * externalIdList.size());
        for (String externalId : externalIdList) {
            items.addAll(generateTsDatapointsObjects(noItems, frequency, externalId));
        }
        return items;
    }

    public static List<TimeseriesPointPost> generateTsDatapointsObjects(int noItems, double frequency, String externalId) {
        List<TimeseriesPointPost> items = new ArrayList<>(noItems);
        Instant timeStamp = Instant.now();

        for (int i = 0; i < noItems; i++) {
            timeStamp = timeStamp.minusMillis(Math.round(1000l / frequency));
            items.add(TimeseriesPointPost.newBuilder()
                        .setExternalId(externalId)
                        .setTimestamp(timeStamp.toEpochMilli())
                        .setValueNum(ThreadLocalRandom.current().nextLong(-10, 20))
                        .build());
        }
        return items;
    }

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

    public static List<Event> generateEvents(int noObjects) {
        List<Event> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(Event.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setStartTime(1552566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setEndTime(1553566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setDescription("generated_event_" + RandomStringUtils.randomAlphanumeric(50))
                    .setType("generated_event")
                    .setSubtype(
                            ThreadLocalRandom.current().nextInt(0,2) == 0 ? "event_sub_type" : "event_sub_type_2")
                    .setSource(sourceValue)
                    .putMetadata("type", DataGenerator.sourceValue)
                    .putMetadata(sourceKey, DataGenerator.sourceValue)
                    .build());
        }
        return objects;
    }

    public static List<Label> generateLabels(int noObjects) {
        List<Label> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(Label.newBuilder()
                    .setExternalId(DataGenerator.sourceValue + RandomStringUtils.randomAlphanumeric(10))
                    .setName(RandomStringUtils.randomAlphanumeric(10))
                    .setDescription("generated_event_" + RandomStringUtils.randomAlphanumeric(50))
                    .build());
        }

        return objects;
    }

    public static List<Relationship> generateRelationships(int noObjects) {
        List<Relationship> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(Relationship.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setStartTime(1552566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setEndTime(1553566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setSourceExternalId("extId_A")
                    .setSourceType(ThreadLocalRandom.current().nextInt(0,2) == 0 ?
                            Relationship.ResourceType.ASSET : Relationship.ResourceType.EVENT)
                    .setTargetExternalId("extId_B")
                    .setTargetType(ThreadLocalRandom.current().nextInt(0,2) == 0 ?
                            Relationship.ResourceType.ASSET : Relationship.ResourceType.EVENT)
                    .setConfidence(ThreadLocalRandom.current().nextFloat())
                    .build());
        }
        return objects;
    }

    public static List<DataSet> generateDataSets(int noObjects) {
        List<DataSet> objects = new ArrayList<>();
        for (int i = 0; i < noObjects; i++) {
            objects.add(DataSet.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("generated-" + RandomStringUtils.randomAlphanumeric(5))
                    .setDescription("Generated description")
                    .putMetadata("type", DataGenerator.sourceValue)
                    .putMetadata(sourceKey, DataGenerator.sourceValue)
                    .build());
        }
        return objects;
    }

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
                            //.addRawTables(ExtractionPipeline.RawTable.newBuilder()
                            //        .setDbName("generated-")
                            //        .setTableName("generated")
                            //        .build())
                            .build());
        }
        return objects;
    }

    public static List<ExtractionPipelineRun> generateExtractionPipelineRuns(int noObjects, String pipelineExtId) {
        List<ExtractionPipelineRun> objects = new ArrayList<>();
        for (int i = 0; i < noObjects; i++) {
            objects.add(ExtractionPipelineRun.newBuilder()
                            .setExternalId(pipelineExtId)
                            .setCreatedTime(Instant.now().toEpochMilli())
                            .setMessage("generated-" + RandomStringUtils.randomAlphanumeric(5))
                            .setStatus(ExtractionPipelineRun.Status.SUCCESS)
                            .build());
            try {
                Thread.sleep(500L);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return objects;
    }

    /*
    Will generate a hierarchy that is 4 levels deep.
     */
    public static List<Asset> generateAssetHierarchy(int noObjects) {
        int NO_HIERARCHY_LEVELS = 5;
        int assetsPerParent = (int) Math.ceil(customLog(NO_HIERARCHY_LEVELS, noObjects));
        List<Asset> hierarchy = new ArrayList<>(noObjects);
        Asset root = generateAssets(1).get(0);
        List<Asset> currentLevel = new ArrayList<>();
        List<Asset> children = new ArrayList<>();
        currentLevel.add(root);
        hierarchy.add(root);
        for (int i = 0; i < NO_HIERARCHY_LEVELS && hierarchy.size() < noObjects; i++) {
            children.clear();
            for (Asset asset : currentLevel) {
                children.addAll(generateChildAssets(assetsPerParent, asset));
            }
            currentLevel.clear();
            currentLevel.addAll(children);
            hierarchy.addAll(children);
        }

        return hierarchy;
    }

    private static List<Asset> generateChildAssets(int noObjects, Asset parent) {
        List<Asset> objects = generateAssets(noObjects);
        List<Asset> resultObjects = new ArrayList<>();
        for (Asset asset : objects) {
            resultObjects.add(asset.toBuilder()
                    .setParentExternalId(parent.getExternalId())
                    .build());
        }
        return resultObjects;
    }

    public static List<Asset> generateAssets(int noObjects) {
        List<Asset> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(Asset.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("generated_asset_" + RandomStringUtils.randomAlphanumeric(5))
                    .setDescription("generated_asset_description_" + RandomStringUtils.randomAlphanumeric(50))
                    .setSource(sourceValue)
                    .putMetadata("type", DataGenerator.sourceValue)
                    .putMetadata(sourceKey, DataGenerator.sourceValue)
                    .build());
        }
        return objects;
    }

    public static List<SecurityCategory> generateSecurityGroups(int noObjects) {
        List<SecurityCategory> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(SecurityCategory.newBuilder()
                    .setName(RandomStringUtils.randomAlphanumeric(10))
                    .build());
        }
        return objects;
    }

    public static List<String> generateListString(int noObjects) {
        List<String> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(StringValue.of(RandomStringUtils.randomAlphanumeric(10)).getValue());
        }
        return objects;
    }

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

    private static double customLog(double base, double logNumber) {
        return Math.log(logNumber) / Math.log(base);
    }


    public static List<ThreeDModel> generate3DModels(int noObjects, long dataSetId) {
        List<ThreeDModel> objects = new ArrayList<>();
        for (int i = 0; i < noObjects; i++) {
            ThreeDModel.Builder builder = ThreeDModel.newBuilder();
            builder.setName("generated-" + RandomStringUtils.randomAlphanumeric(5));
            builder.setDataSetId(dataSetId);
            builder.setCreatedTime(1552566113 + ThreadLocalRandom.current().nextInt(10000));
            objects.add(builder.build());
        }
        return objects;
    }
}
