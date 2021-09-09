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
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

public class DiagramResponseParser {
    static final String logPrefix = "DiagramResponseParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a P&ID annotation detection response json string to <code>pnid_status</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static DiagramResponse ParseDiagramAnnotationResponse(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        DiagramResponse.Builder diagramBuilder = DiagramResponse.newBuilder();

        if (root.path("fileId").isIntegralNumber()) {
            diagramBuilder.setFileId(root.get("fileId").longValue());
        }
        if (root.path("fileExternalId").isTextual()) {
            diagramBuilder.setFileExternalId(root.get("fileExternalId").textValue());
        }

        if (root.path("annotations").isArray()) {
            for (JsonNode node : root.path("annotations")) {
                Annotation.Builder annotationBuilder = Annotation.newBuilder();
                if (node.path("text").isTextual()) {
                    annotationBuilder.setText(node.get("text").textValue());
                }
                if (node.path("confidence").isFloatingPointNumber()) {
                    annotationBuilder.setConfidence(node.get("confidence").doubleValue());
                }
                if (node.path("region").isObject()) {
                    JsonNode regionNode = node.get("region");
                    Annotation.Region.Builder regionBuilder = Annotation.Region.newBuilder();
                    if (regionNode.path("shape").isTextual()) {
                        regionBuilder.setShape(node.path("shape").textValue());
                    }
                    if (regionNode.path("vertices").isArray()) {
                        for (JsonNode vertexNode : regionNode.path("vertices")) {
                            Annotation.Vertex.Builder vertexBuilder = Annotation.Vertex.newBuilder();
                            if (vertexNode.path("x").isFloatingPointNumber()) {
                                vertexBuilder.setX(vertexNode.path("x").doubleValue());
                            }
                            if (vertexNode.path("y").isFloatingPointNumber()) {
                                vertexBuilder.setY(vertexNode.path("y").doubleValue());
                            }
                            regionBuilder.addVertices(vertexBuilder.build());
                        }
                    }
                    if (regionNode.path("page").isIntegralNumber()) {
                        regionBuilder.setPage(node.path("page").intValue());
                    }

                    annotationBuilder.setRegion(regionBuilder.build());
                }

                if (node.path("entities").isArray()) {
                    for (JsonNode entity : node.path("entities")) {
                        if (entity.isObject()) {
                            Struct.Builder structBuilder = Struct.newBuilder();
                            JsonFormat.parser().merge(entity.toString(), structBuilder);
                            annotationBuilder.addEntities(structBuilder.build());
                        }
                    }
                }

                diagramBuilder.addItems(annotationBuilder);
            }
        } else {
            throw new Exception(DiagramResponseParser.buildErrorMessage("annotations", json));
        }

        return diagramBuilder.build();
    }

    /**
     * Parses a diagram convert response json string to {@link ConvertResponse} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ConvertResponse ParseDiagramConvertResponse(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        ConvertResponse.Builder convertBuilder = ConvertResponse.newBuilder();

        if (root.path("fileId").isIntegralNumber()) {
            convertBuilder.setFileId(root.get("fileId").longValue());
        }
        if (root.path("fileExternalId").isTextual()) {
            convertBuilder.setFileExternalId(root.get("fileExternalId").textValue());
        }
        if (root.path("results").isArray()) {
            for (JsonNode node : root.path("results")) {
                ConvertResponse.Result.Builder resultBuilder = ConvertResponse.Result.newBuilder();
                if (node.path("page").isIntegralNumber()) {
                    resultBuilder.setPage(node.path("page").intValue());
                }
                if (node.path("svgUrl").isTextual()) {
                    resultBuilder.setSvgUrl(node.path("svgUrl").textValue());
                }
                if (node.path("pngUrl").isTextual()) {
                    resultBuilder.setPngUrl(node.path("svgUrl").textValue());
                }
                if (node.path("errorMessage").isTextual()) {
                    resultBuilder.setErrorMessage(node.path("errorMessage").textValue());
                }
                convertBuilder.addResults(resultBuilder.build());
            }
        }

        return convertBuilder.build();
    }

    private static String buildErrorMessage(String fieldName, String inputElement) {
        return logPrefix + "Unable to parse attribute: "+ fieldName + ". Item payload: "
                + inputElement.substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH));
    }
}
