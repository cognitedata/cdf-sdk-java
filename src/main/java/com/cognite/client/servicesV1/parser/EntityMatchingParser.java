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

import com.cognite.client.dto.EntityMatch;
import com.cognite.client.dto.EntityMatchModel;
import com.cognite.client.dto.EntityMatchResult;
import com.cognite.client.dto.MatchField;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * Methods for parsing entity matching results json into typed objects.
 */
public class EntityMatchingParser {
    static final String logPrefix = "EntityMatchingParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an entity matching result json string into a typed {@link EntityMatchResult}.
     * @param json
     * @return
     * @throws Exception
     */
    public static EntityMatchResult parseEntityMatchResult(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        EntityMatchResult.Builder responseBuilder = EntityMatchResult.newBuilder();

        if (root.path("source").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(root.path("source").toString(), structBuilder);
            responseBuilder.setSource(structBuilder.build());
        } else {
            throw new Exception(logPrefix + "Unable to parse result item. "
                    + "Result does not contain a valid [source] node. "
                    + "Source: " + root.path("source").getNodeType());
        }

        if (root.path("matches").isArray()) {
            for (JsonNode node : root.path("matches")) {
                if (node.isObject()) {
                    EntityMatch.Builder entityMatchBuilder = EntityMatch.newBuilder();
                    if (node.path("target").isObject()) {
                        Struct.Builder structBuilder = Struct.newBuilder();
                        JsonFormat.parser().merge(node.path("target").toString(), structBuilder);
                        entityMatchBuilder.setTarget(structBuilder.build());
                    } else {
                        throw new Exception(logPrefix + "Unable to parse result item. "
                                + "Result does not contain a valid [target] node. "
                                + "Target: " + node.path("target").getNodeType());
                    }

                    if (node.path("score").isNumber()) {
                        entityMatchBuilder.setScore(DoubleValue.of(node.path("score").doubleValue()));
                    }
                    responseBuilder.addMatches(entityMatchBuilder.build());
                } else {
                    throw new Exception(logPrefix + "Unable to parse result item. "
                            + "Result does not contain a valid [matches] entry node. "
                            + "matches: " + node.getNodeType());
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse result item. "
                    + "Result does not contain a valid [matches] array node. "
                    + "matches array: " + root.path("matches").getNodeType());
        }

        return responseBuilder.build();
    }

    public static EntityMatchModel parseEntityMatchModel(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        EntityMatchModel.Builder responseBuilder = EntityMatchModel.newBuilder();
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        if (root.path("id").isIntegralNumber()) {
            responseBuilder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: id. Item excerpt: " + jsonExcerpt;
            throw new Exception(message);
        }

        if (root.path("status").isTextual()) {
            responseBuilder.setStatus(StringValue.of(root.get("status").textValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: status. Item excerpt: " + jsonExcerpt;
            throw new Exception(message);
        }

        if (root.path("createdTime").isIntegralNumber()) {
            responseBuilder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: createdTime. Item excerpt: " + jsonExcerpt;
            throw new Exception(message);
        }

        if (root.path("startTime").isIntegralNumber()) {
            responseBuilder.setStartTime(Int64Value.of(root.get("startTime").longValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: startTime. Item excerpt: " + jsonExcerpt;
            throw new Exception(message);
        }

        if (root.path("statusTime").isIntegralNumber()) {
            responseBuilder.setStatusTime(Int64Value.of(root.get("statusTime").longValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: statusTime. Item excerpt: " + jsonExcerpt;
            throw new Exception(message);
        }

        // Optional attributes
        if (root.path("externalId").isTextual()) {
            responseBuilder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }
        if (root.path("name").isTextual()) {
            responseBuilder.setName(StringValue.of(root.get("name").textValue()));
        }
        if (root.path("description").isTextual()) {
            responseBuilder.setDescription(StringValue.of(root.get("description").textValue()));
        }
        if (root.path("featureType").isTextual()) {
            responseBuilder.setFeatureType(StringValue.of(root.get("featureType").textValue()));
        }
        if (root.path("matchFields").isArray()) {
            for (JsonNode node : root.path("matchFields")) {
                MatchField.Builder matchFieldBuilder = MatchField.newBuilder();
                if (node.path("source").isTextual()) {
                    matchFieldBuilder.setSource(node.get("source").textValue());
                } else {
                    String message = logPrefix + "Unable to parse attribute: matchField.source. Item excerpt: " + jsonExcerpt;
                    throw new Exception(message);
                }
                if (node.path("target").isTextual()) {
                    matchFieldBuilder.setTarget(node.get("target").textValue());
                } else {
                    String message = logPrefix + "Unable to parse attribute: matchField.target. Item excerpt: " + jsonExcerpt;
                    throw new Exception(message);
                }
                responseBuilder.addMatchFields(matchFieldBuilder.build());
            }
        }
        if (root.path("classifier").isTextual()) {
            responseBuilder.setClassifier(StringValue.of(root.get("classifier").textValue()));
        }
        if (root.path("ignoreMissingFields").isBoolean()) {
            responseBuilder.setIgnoreMissingFields(BoolValue.of(root.get("ignoreMissingFields").booleanValue()));
        }

        return responseBuilder.build();
    }
}
