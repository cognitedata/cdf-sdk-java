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

import com.cognite.client.dto.ExtractionPipeline;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.dto.Relationship;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parse file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class ExtractionPipelineParser {
    private static final String logPrefix = "ExtractionPipelineParser - ";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ImmutableBiMap<String, ExtractionPipelineRun.Status> statusMap = ImmutableBiMap
            .<String, ExtractionPipelineRun.Status>builder()
            .put("success", ExtractionPipelineRun.Status.SUCCESS)
            .put("failure", ExtractionPipelineRun.Status.FAILURE)
            .put("seen", ExtractionPipelineRun.Status.SEEN)
            .build();

    /**
     * Parses an extraction pipeline json string to {@link ExtractionPipeline} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ExtractionPipeline parseExtractionPipeline(String json) throws Exception {
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
        JsonNode root = objectMapper.readTree(json);
        ExtractionPipeline.Builder pipelineBuilder = ExtractionPipeline.newBuilder();

        // Required attributes.
        if (root.path("externalId").isTextual()) {
            pipelineBuilder.setExternalId(root.get("externalId").textValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("externalId", jsonExcerpt));
        }
        if (root.path("name").isTextual()) {
            pipelineBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("name", jsonExcerpt));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            pipelineBuilder.setDataSetId(root.get("dataSetId").longValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("dataSetId", jsonExcerpt));
        }

        // The rest of the attributes are optional.
        if (root.path("id").isIntegralNumber()) {
            pipelineBuilder.setId(root.get("id").longValue());
        }
        if (root.path("description").isTextual()) {
            pipelineBuilder.setDescription(root.get("description").textValue());
        }
        if (root.path("rawTables").isArray()) {
            for (JsonNode node : root.path("rawTables")) {
                if (node.isObject()
                        && node.path("dbName").isTextual()
                        && node.path("tableName").isTextual()) {
                    pipelineBuilder.addRawTables(ExtractionPipeline.RawTable.newBuilder()
                            .setDbName(node.path("dbName").textValue())
                            .setTableName(node.path("tableName").textValue())
                            .build());
                } else {
                    throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("rawTables", jsonExcerpt));
                }
            }
        }
        if (root.path("schedule").isTextual()) {
            pipelineBuilder.setSchedule(root.get("schedule").textValue());
        }
        if (root.path("contacts").isArray()) {
            for (JsonNode node : root.path("contacts")) {
                if (node.isObject()) {
                    ExtractionPipeline.Contact.Builder contactBuilder = ExtractionPipeline.Contact.newBuilder();
                    if (node.path("name").isTextual()) {
                        contactBuilder.setName(node.path("name").textValue());
                    }
                    if (node.path("email").isTextual()) {
                        contactBuilder.setEmail(node.path("email").textValue());
                    }
                    if (node.path("role").isTextual()) {
                        contactBuilder.setRole(node.path("role").textValue());
                    }
                    if (node.path("sendNotification").isBoolean()) {
                        contactBuilder.setSendNotification(node.path("sendNotification").booleanValue());
                    }
                    pipelineBuilder.addContacts(contactBuilder.build());
                } else {
                    throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("contacts", jsonExcerpt));
                }
            }
        }
        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    pipelineBuilder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }
        if (root.path("source").isTextual()) {
            pipelineBuilder.setSource(root.get("source").textValue());
        }
        if (root.path("documentation").isTextual()) {
            pipelineBuilder.setDocumentation(root.get("documentation").textValue());
        }
        if (root.path("lastSuccess").isIntegralNumber()) {
            pipelineBuilder.setLastSuccess(root.get("lastSuccess").longValue());
        }
        if (root.path("lastFailure").isIntegralNumber()) {
            pipelineBuilder.setLastFailure(root.get("lastFailure").longValue());
        }
        if (root.path("lastMessage").isTextual()) {
            pipelineBuilder.setLastMessage(root.get("lastMessage").textValue());
        }
        if (root.path("lastSeen").isIntegralNumber()) {
            pipelineBuilder.setLastSeen(root.get("lastSeen").longValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            pipelineBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            pipelineBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }
        if (root.path("createdBy").isTextual()) {
            pipelineBuilder.setCreatedBy(root.get("createdBy").textValue());
        }

        return pipelineBuilder.build();
    }

    /**
     * Parses an extraction pipeline json string to {@link ExtractionPipelineRun} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ExtractionPipelineRun parseExtractionPipelineRun(String json) throws Exception {
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
        JsonNode root = objectMapper.readTree(json);
        ExtractionPipelineRun.Builder pipelineRunBuilder = ExtractionPipelineRun.newBuilder();

        // Required attributes.
        if (root.path("status").isTextual() && statusMap.containsKey(root.get("status").textValue())) {
            pipelineRunBuilder.setStatus(statusMap.get(root.get("status").textValue()));
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("status", jsonExcerpt));
        }

        // The rest of the attributes are optional.
        if (root.path("id").isIntegralNumber()) {
            pipelineRunBuilder.setId(root.get("id").longValue());
        }
        if (root.path("message").isTextual()) {
            pipelineRunBuilder.setMessage(root.get("message").textValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            pipelineRunBuilder.setCreatedTime(root.get("createdTime").longValue());
        }

        return pipelineRunBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link ExtractionPipeline}.
     *
     * An insert item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(ExtractionPipeline element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        //Preconditions.checkState(element.hasExternalId(), "Input must contain externalId.");
        //Preconditions.checkState(element.hasName(), "Input must contain name.");
        //Preconditions.checkState(element.hasDataSetId(), "Input must contain dataSetId.");

        // Add all the mandatory attributes
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("name", element.getName())
                .put("dataSetId", element.getDataSetId());

        // Add optional attributes
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription());
        }
        if (element.getRawTablesCount() > 0) {
            List<Map<String, String>> rawTables = new ArrayList<>();
            for (ExtractionPipeline.RawTable table : element.getRawTablesList()) {
                rawTables.add(ImmutableMap.of(
                        "dbName", table.getDbName(),
                        "tableName", table.getTableName()));
            }
            mapBuilder.put("rawTables", rawTables);
        }
        if (element.hasSchedule()) {
            mapBuilder.put("schedule", element.getSchedule());
        }
        if (element.getContactsCount() > 0) {
            List<Map<String, Object>> contacts = new ArrayList<>();
            for (ExtractionPipeline.Contact contact : element.getContactsList()) {
                Map<String, Object> contactMap = new HashMap<>();
                contactMap.put("sendNotification", contact.getSendNotification());
                if (contact.hasName()) {
                    contactMap.put("name", contact.getName());
                }
                if (contact.hasEmail()) {
                    contactMap.put("email", contact.getEmail());
                }
                if (contact.hasRole()) {
                    contactMap.put("role", contact.getRole());
                }
                contacts.add(contactMap);
            }
            mapBuilder.put("contacts", contacts);
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.hasSource()) {
            mapBuilder.put("source", element.getSource());
        }
        if (element.hasDocumentation()) {
            mapBuilder.put("documentation", element.getDocumentation());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link ExtractionPipelineRun}.
     *
     * An insert item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(ExtractionPipelineRun element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkState(element.hasExternalId(), "Input must contain externalId.");

        // Add all the mandatory attributes
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("status", statusMap.inverse().get(element.getStatus()));

        // Add optional attributes
        if (element.hasMessage()) {
            mapBuilder.put("message", element.getMessage());
        }
        if (element.hasCreatedTime()) {
            mapBuilder.put("createdTime", element.getCreatedTime());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from {@link ExtractionPipeline}.
     *
     * An update item object updates an existing extraction pipeline object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(ExtractionPipeline element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        //Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
        //        "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Add id reference
        mapBuilder.put("externalId", element.getExternalId());

        // Add the update fields
        updateNodeBuilder
                .put("name", ImmutableMap.of("set", element.getName()))
                .put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));

        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        }
        if (element.hasSchedule()) {
            updateNodeBuilder.put("schedule", ImmutableMap.of("set", element.getSchedule()));
        }
        if (element.getRawTablesCount() > 0) {
            List<Map<String, String>> rawTables = new ArrayList<>();
            for (ExtractionPipeline.RawTable table : element.getRawTablesList()) {
                rawTables.add(ImmutableMap.of(
                        "dbName", table.getDbName(),
                        "tableName", table.getTableName()));
            }
            updateNodeBuilder.put("rawTables", ImmutableMap.of("add", rawTables));
        }
        if (element.getContactsCount() > 0) {
            List<Map<String, Object>> contacts = new ArrayList<>();
            for (ExtractionPipeline.Contact contact : element.getContactsList()) {
                Map<String, Object> contactMap = new HashMap<>();
                contactMap.put("sendNotification", contact.getSendNotification());
                if (contact.hasName()) {
                    contactMap.put("name", contact.getName());
                }
                if (contact.hasEmail()) {
                    contactMap.put("email", contact.getEmail());
                }
                if (contact.hasRole()) {
                    contactMap.put("role", contact.getRole());
                }
                contacts.add(contactMap);
            }
            updateNodeBuilder.put("contacts", ImmutableMap.of("add", contacts));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        }
        if (element.hasDocumentation()) {
            updateNodeBuilder.put("documentation", ImmutableMap.of("set", element.getDocumentation()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request replace item object from {@link ExtractionPipeline}.
     *
     * A replace item object replaces an existing extraction pipeline object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(ExtractionPipeline element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        //Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
        //        "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Add id reference
        mapBuilder.put("externalId", element.getExternalId());

        // Add the update fields
        updateNodeBuilder
                .put("name", ImmutableMap.of("set", element.getName()))
                .put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));

        // Add fields that can be set to null. Please note that extraction pipelines do not use an explicit
        // "setNull", but rather an implicit empty string.
        updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        updateNodeBuilder.put("schedule", ImmutableMap.of("set", element.getSchedule()));
        updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        updateNodeBuilder.put("documentation", ImmutableMap.of("set", element.getDocumentation()));

        List<Map<String, String>> rawTables = new ArrayList<>();
        for (ExtractionPipeline.RawTable table : element.getRawTablesList()) {
            rawTables.add(ImmutableMap.of(
                    "dbName", table.getDbName(),
                    "tableName", table.getTableName()));
        }
        updateNodeBuilder.put("rawTables", ImmutableMap.of("set", rawTables));

        List<Map<String, Object>> contacts = new ArrayList<>();
        for (ExtractionPipeline.Contact contact : element.getContactsList()) {
            Map<String, Object> contactMap = new HashMap<>();
            contactMap.put("sendNotification", contact.getSendNotification());
            if (contact.hasName()) {
                contactMap.put("name", contact.getName());
            }
            if (contact.hasEmail()) {
                contactMap.put("email", contact.getEmail());
            }
            if (contact.hasRole()) {
                contactMap.put("role", contact.getRole());
            }
            contacts.add(contactMap);
        }
        updateNodeBuilder.put("contacts", ImmutableMap.of("set", contacts));

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Returns the string representation of a pipeline run status.
     * @param status
     * @return
     */
    public static String toString(ExtractionPipelineRun.Status status) {
        return statusMap.inverse().get(status);
    }

    /**
     * Tries to parse a string into a {@code ExtractionPipelineRun.Status}. If the string cannot be parsed, the returned
     * {@code Optional} will be empty.
     * @param status
     * @return
     */
    public static Optional<ExtractionPipelineRun.Status> parsePipelineRunStatus(String status) {
        return Optional.ofNullable(statusMap.get(status));
    }

    private static String buildParsingExceptionString(String attribute, String json) {
        return logPrefix + "Unable to parse attribute [" + attribute + "]. Item excerpt: " + json;
    }
}
