package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Transformation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class TransformationJobsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static Transformation.Job parseTransformationJobs(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        Transformation.Job.Builder tmBuilder = Transformation.Job.newBuilder();

        if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    public static void extractNodes(Transformation.Job.Builder tmBuilder, JsonNode root) {
        if (root.path("id").isNumber()) {
            tmBuilder.setId(root.get("id").intValue());
        }
        if(root.path("uuid").isTextual()) {
            tmBuilder.setUuid(root.path("uuid").textValue());
        }
        if(root.path("transformationId").isNumber()) {
            tmBuilder.setTransformationId(root.path("transformationId").intValue());
        }
        if(root.path("transformationExternalId").isTextual()) {
            tmBuilder.setTransformationExternalId(root.path("transformationExternalId").textValue());
        }
        if(root.path("sourceProject").isTextual()) {
            tmBuilder.setSourceProject(root.path("sourceProject").textValue());
        }
        if(root.path("destinationProject").isTextual()) {
            tmBuilder.setDestinationProject(root.path("destinationProject").textValue());
        }
        if (root.path("destination").isObject()) {

            if (root.path("destination").size() == 1) {
                JsonNode dataSourceNode = root.path("destination");
                tmBuilder.setDestination(Transformation.Destination.newBuilder()
                        .setType(dataSourceNode.get("type").textValue())
                        .build());
            } else {
                if (root.path("destination").path("type").textValue().equals("raw")) {
                    JsonNode rawDataSourceNode = root.path("destination");
                    Transformation.Destination.Builder builderDest = Transformation.Destination.newBuilder();
                    if (rawDataSourceNode.path("type").isTextual()) {
                        builderDest.setType(rawDataSourceNode.path("type").textValue());
                    }
                    if (rawDataSourceNode.path("database").isTextual()) {
                        builderDest.setDatabase(rawDataSourceNode.path("database").textValue());
                    }
                    if (rawDataSourceNode.path("table").isTextual()) {
                        builderDest.setTable(rawDataSourceNode.path("table").textValue());
                    }
                    tmBuilder.setDestination(builderDest.build());
                } else if (root.path("destination").path("type").textValue().equals("sequence_rows")) {
                    JsonNode sequenceRowDataSourceNode = root.path("destination");
                    Transformation.Destination.Builder builderDest = Transformation.Destination.newBuilder();
                    if (sequenceRowDataSourceNode.path("type").isTextual()) {
                        builderDest.setType(sequenceRowDataSourceNode.path("type").textValue());
                    }
                    if (sequenceRowDataSourceNode.path("externalId").isTextual()) {
                        builderDest.setExternalId(sequenceRowDataSourceNode.path("externalId").textValue());
                    }
                    tmBuilder.setDestination(builderDest.build());
                } else if (root.path("destination").path("type").textValue().equals("instances")) {
                    JsonNode instanceDataSourceNode = root.path("destination");
                    Transformation.Destination.Builder builderDest = Transformation.Destination.newBuilder();

                    builderDest.setType(instanceDataSourceNode.path("type").textValue());

                    if (instanceDataSourceNode.path("dataModel").isObject()) {
                        JsonNode dataModelSourceNode = instanceDataSourceNode.path("dataModel");
                        Transformation.Destination.Datamodel.Builder dataModelBuilderDest = Transformation.Destination.Datamodel.newBuilder();
                        JsonNode space = dataModelSourceNode.path("space");
                        JsonNode externalId = dataModelSourceNode.path("externalId");
                        JsonNode version = dataModelSourceNode.path("version");
                        JsonNode destinationType = dataModelSourceNode.path("destinationType");
                        if (space.isTextual())
                            dataModelBuilderDest.setSpace(space.textValue());
                        if (externalId.isTextual())
                            dataModelBuilderDest.setExternalId(externalId.textValue());
                        if (version.isTextual())
                            dataModelBuilderDest.setVersion(version.textValue());
                        if (destinationType.isTextual())
                            dataModelBuilderDest.setDestinationType(destinationType.textValue());

                        builderDest.setDataModel(dataModelBuilderDest.build());
                    }
                    JsonNode instanceSpace = instanceDataSourceNode.path("instanceSpace");
                    if (instanceSpace.isTextual())
                        builderDest.setInstanceSpace(instanceSpace.textValue());

                    tmBuilder.setDestination(builderDest.build());
                }
            }
        }

        if (root.path("conflictMode").isTextual()) {
            tmBuilder.setConflictMode(root.path("conflictMode").textValue());
        }
        if (root.path("query").isTextual()) {
            tmBuilder.setConflictMode(root.path("query").textValue());
        }
        if (root.path("createdTime").isLong()) {
            tmBuilder.setCreatedTime(root.path("createdTime").longValue());
        }
        if (root.path("startedTime").isLong()) {
            tmBuilder.setStartedTime(root.path("startedTime").longValue());
        }
        if (root.path("finishedTime").isLong()) {
            tmBuilder.setFinishedTime(root.path("finishedTime").longValue());
        }
        if (root.path("lastSeenTime").isLong()) {
            tmBuilder.setLastSeenTime(root.path("lastSeenTime").longValue());
        }
        if (root.path("error").isTextual()) {
            tmBuilder.setError(root.path("error").textValue());
        }
        if (root.path("ignoreNullFields").isBoolean()) {
            tmBuilder.setIgnoreNullFields(root.path("ignoreNullFields").booleanValue());
        }
        if (root.path("status").isTextual()) {
            tmBuilder.setStatus(root.path("status").textValue());
        }
    }
}
