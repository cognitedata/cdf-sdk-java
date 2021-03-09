/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;

import java.util.*;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing {@code Sequence} objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class SequenceParser {
    static final String logPrefix = "SequenceParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ImmutableBiMap<String, SequenceColumn.ValueType> valueTypeMap = ImmutableBiMap
            .<String, SequenceColumn.ValueType>builder()
            .put("DOUBLE", SequenceColumn.ValueType.DOUBLE)
            .put("LONG", SequenceColumn.ValueType.LONG)
            .put("STRING", SequenceColumn.ValueType.STRING)
            .build();

    /**
     * Parses a sequence header json string to {@link SequenceMetadata} proto object.
     *
     * @param json The json representation of a sequence header
     * @return The sequence header as a typed object
     * @throws Exception
     */
    public static SequenceMetadata parseSequenceMetadata(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        SequenceMetadata.Builder builder = SequenceMetadata.newBuilder();
        String itemExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        // A Sequence metadata object must contain an id and columns.
        if (root.path("id").isIntegralNumber()) {
            builder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: id. Item excerpt: " + itemExcerpt);
        }

        if (root.path("columns").isArray()) {
            for (JsonNode node : root.path("columns")) {
                if (node.isContainerNode()) {
                    builder.addColumns(SequenceParser.parseSequenceColumn(node.toString()));
                } else {
                    throw new Exception(logPrefix + "Unable to parse attribute: columns. "
                            + "The column is not a json object node. Item excerpt: " + itemExcerpt);
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: columns. Item excerpt: " + itemExcerpt);
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            builder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }
        if (root.path("name").isTextual()) {
            builder.setName(StringValue.of(root.get("name").textValue()));
        }
        if (root.path("description").isTextual()) {
            builder.setDescription(StringValue.of(root.get("description").textValue()));
        }
        if (root.path("assetId").isIntegralNumber()) {
            builder.setAssetId(Int64Value.of(root.get("assetId").longValue()));
        }
        if (root.path("createdTime").isIntegralNumber()) {
            builder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            builder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            builder.setDataSetId(Int64Value.of(root.get("dataSetId").longValue()));
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    builder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        return builder.build();
    }

    /**
     * Parses a sequence body json string to {@link SequenceBody} proto object.
     *
     * @param json The json representation of a sequence body
     * @return The sequence body as a typed object
     * @throws Exception
     */
    public static SequenceBody parseSequenceBody(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        SequenceBody.Builder builder = SequenceBody.newBuilder();
        String itemExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        // A Sequence metadata object must contain an id and columns.
        if (root.path("id").isIntegralNumber()) {
            builder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: id. Item excerpt: " + itemExcerpt);
        }

        if (root.path("columns").isArray()) {
            for (JsonNode node : root.path("columns")) {
                if (node.isContainerNode()) {
                    builder.addColumns(SequenceParser.parseSequenceColumn(node.toString()));
                } else {
                    throw new Exception(logPrefix + "Unable to parse attribute: columns. "
                            + "The column is not a json object node. Item excerpt: " + itemExcerpt);
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: columns. Item excerpt: " + itemExcerpt);
        }

        if (root.path("rows").isArray()) {
            for (JsonNode node : root.path("rows")) {
                if (node.isContainerNode()) {
                    builder.addRows(SequenceParser.parseSequenceRow(node.toString()));
                } else {
                    throw new Exception(logPrefix + "Unable to parse attribute: rows. "
                            + "The row is not a json object node. Item excerpt: " + itemExcerpt);
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: rows. Item excerpt: " + itemExcerpt);
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            builder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }

        return builder.build();
    }

    /**
     * Parses a sequence column json into a typed {@link SequenceColumn} object
     *
     * @param json The sequence column json object
     * @return The typed sequence column
     * @throws Exception
     */
    private static SequenceColumn parseSequenceColumn(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        SequenceColumn.Builder builder = SequenceColumn.newBuilder();
        String itemExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        // A Sequence column object must contain an externalId.
        if (root.path("externalId").isTextual()) {
            builder.setExternalId(root.get("externalId").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: column.externalId. Item excerpt: " + itemExcerpt);
        }

        // The rest of the attributes are optional.
        if (root.path("name").isTextual()) {
            builder.setName(StringValue.of(root.get("name").textValue()));
        }
        if (root.path("description").isTextual()) {
            builder.setDescription(StringValue.of(root.get("description").textValue()));
        }
        if (root.path("valueType").isTextual()) {
            Optional<SequenceColumn.ValueType> valueType = SequenceParser.parseValueType(root.get("valueType").textValue());
            if (valueType.isPresent()) {
                builder.setValueType(valueType.get());
            } else {
                throw new Exception(logPrefix + "Unable to parse attribute: column.valueType. Item excerpt: " + itemExcerpt);
            }
        }
        if (root.path("createdTime").isIntegralNumber()) {
            builder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            builder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    builder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        return builder.build();
    }

    /**
     * Parses a sequence row json into a typed {@link SequenceRow} object
     *
     * @param json The sequence row json object
     * @return The typed sequence row
     * @throws Exception
     */
    private static SequenceRow parseSequenceRow(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        SequenceRow.Builder builder = SequenceRow.newBuilder();
        String itemExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        // A Sequence row object must contain a row number and a set of values.
        if (root.path("rowNumber").isIntegralNumber()) {
            builder.setRowNumber(root.get("rowNumber").longValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: row.rowNumber. Item excerpt: " + itemExcerpt);
        }

        if (root.path("values").isArray()) {
            for (JsonNode node : root.path("values")) {
                if (node.isNumber()) {
                    builder.addValues(Values.of(node.doubleValue()));
                } else if (node.isTextual()) {
                    builder.addValues(Values.of(node.textValue()));
                } else if (node.isNull()) {
                    builder.addValues(Values.ofNull());
                } else {
                    throw new Exception(logPrefix + "Unable to parse attribute: row.values. Item excerpt: " + itemExcerpt);
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: row.values. Item excerpt: " + itemExcerpt);
        }

        return builder.build();
    }

    /**
     * Builds a request insert item object from <code>SequenceMetadata</code>.
     *
     * An insert item object creates a new sequence header data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(SequenceMetadata element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        }
        if (element.hasName()) {
            mapBuilder.put("name", element.getName().getValue());
        }
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription().getValue());
        }
        if (element.hasAssetId()) {
            mapBuilder.put("assetId", element.getAssetId().getValue());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.getColumnsCount() > 0) {
            List<Map<String, Object>> columnList = new ArrayList<>();
            for (SequenceColumn column : element.getColumnsList()) {
                columnList.add(SequenceParser.toRequestInsertItem(column));
            }
            mapBuilder.put("columns", columnList);
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link SequenceBody}.
     *
     * An insert item object creates a set of new rows for a {@code sequence} in the
     * Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(SequenceBody element) throws Exception {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                logPrefix + "Sequence rows / body must have externalId or Id in order to be upserted.");
        Preconditions.checkArgument(element.getColumnsCount() > 0,
                logPrefix + "Sequences rows / body must specify a set of columns to write to.");
        Preconditions.checkArgument(element.getRowsCount() > 0,
                logPrefix + "Sequences rows / body must contain a set of rows to write.");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.getColumnsCount() > 0) {
            List<String> columnList = new ArrayList<>(element.getColumnsCount());
            element.getColumnsList().forEach(column -> columnList.add(column.getExternalId()));
            mapBuilder.put("columns", columnList);
        }

        if (element.getRowsCount() > 0) {
            List<Map<String, Object>> rowList = new ArrayList<>(element.getRowsCount());
            for (SequenceRow row : element.getRowsList()) {
                Map<String, Object> rowMap = new HashMap<>(2);
                rowMap.put("rowNumber", row.getRowNumber());

                List<Object> valueList = new ArrayList<>(row.getValuesCount());
                if (row.getValuesList().isEmpty()) {
                    // Empty row values is not allowed
                    throw new Exception(logPrefix + String.format("Row has no values. ExternalId: %s, row number: %d",
                            element.getExternalId().getValue(),
                            row.getRowNumber()));
                }
                for (Value value : row.getValuesList()) {
                    if (value.getKindCase() == Value.KindCase.NUMBER_VALUE) {
                        valueList.add(value.getNumberValue());
                    } else if (value.getKindCase() == Value.KindCase.STRING_VALUE) {
                        valueList.add(value.getStringValue());
                    } else if (value.getKindCase() == Value.KindCase.NULL_VALUE) {
                        valueList.add(null);
                    } else {
                        // illegal value type
                        throw new Exception(logPrefix + "Illegal value type for row: "
                                + value.getKindCase()
                                + " . Value type must be numeric or string.");
                    }
                }
                rowMap.put("values", valueList);
                rowList.add(rowMap);
            }
            mapBuilder.put("rows", rowList);
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link SequenceColumn}.
     *
     * An insert item object creates a new sequence column data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static ImmutableMap<String, Object> toRequestInsertItem(SequenceColumn element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        mapBuilder.put("externalId", element.getExternalId());
        mapBuilder.put("valueType", SequenceParser.toString(element.getValueType()));

        if (element.hasName()) {
            mapBuilder.put("name", element.getName().getValue());
        }
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription().getValue());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from {@link SequenceMetadata}.
     *
     * An update item object updates an existing sequence header object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(SequenceMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                logPrefix + "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.hasName()) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName().getValue()));
        }
        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription().getValue()));
        }
        if (element.hasAssetId()) {
            updateNodeBuilder.put("assetId", ImmutableMap.of("set", element.getAssetId().getValue()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link SequenceMetadata}.
     *
     * A replace item object replaces an existingTS header object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(SequenceMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                logPrefix + "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.hasName()) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName().getValue()));
        } else {
            updateNodeBuilder.put("name", ImmutableMap.of("setNull", true));
        }

        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription().getValue()));
        } else {
            updateNodeBuilder.put("description", ImmutableMap.of("setNull", true));
        }

        if (element.hasAssetId()) {
            updateNodeBuilder.put("assetId", ImmutableMap.of("set", element.getAssetId().getValue()));
        } else {
            updateNodeBuilder.put("assetId", ImmutableMap.of("setNull", true));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request delete rows object from <code>SequenceBody</code>.
     *
     * An delete rows object removes a set of rows from a sequence in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestDeleteRowsItem(SequenceBody element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                logPrefix + "Sequence rows / body must have externalId or Id.");
        Preconditions.checkArgument(element.getRowsCount() > 0,
                logPrefix + "Sequences rows / body must contain a set of rows to delete.");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.getRowsCount() > 0) {
            List<Long> rowList = new ArrayList<>(element.getRowsCount());
            element.getRowsList().forEach(row -> {
                rowList.add(row.getRowNumber());
            });
            mapBuilder.put("rows", rowList);
        }

        return mapBuilder.build();
    }

    /**
     * Returns the string representation of a {@link SequenceColumn.ValueType}.
     * @param valueType The value type
     * @return The string representation of the {@link SequenceColumn.ValueType}
     */
    public static String toString(SequenceColumn.ValueType valueType) {
        return valueTypeMap.inverse().get(valueType);
    }

    /**
     * Tries to parse a string into a {@link SequenceColumn.ValueType}. If the string
     * cannot be parsed, the returned {@link Optional} will be empty.
     *
     * @param type The string to be parsed into a {@link SequenceColumn.ValueType}
     * @return an {@link Optional} carrying the {@link SequenceColumn.ValueType}
     */
    public static Optional<SequenceColumn.ValueType> parseValueType(String type) {
        return Optional.ofNullable(valueTypeMap.get(type));
    }
}
