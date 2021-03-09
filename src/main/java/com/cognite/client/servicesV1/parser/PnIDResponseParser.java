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
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

public class PnIDResponseParser {
    static final String logPrefix = "PnIDStatusParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a P&ID annotation detection response json string to <code>pnid_status</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static PnIDResponse ParsePnIDAnnotationResponse(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        PnIDResponse.Builder pnIDBuilder = PnIDResponse.newBuilder();
        BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Annotation.Builder annotationBuilder = Annotation.newBuilder();

        if (root.path("fileId").isIntegralNumber()) {
            pnIDBuilder.setFileId(Int64Value.of(root.get("fileId").longValue()));
        }
        if (root.path("fileExternalId").isTextual()) {
            pnIDBuilder.setFileExternalId(StringValue.of(root.get("fileExternalId").textValue()));
        }

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                if (node.path("boundingBox").isObject()) {
                    if (node.path("boundingBox").path("xMax").isFloatingPointNumber()) {
                        boundingBoxBuilder.setXMax(node.path("boundingBox").path("xMax").doubleValue());
                    } else {
                        throw new Exception(PnIDResponseParser.buildErrorMessage("boundingBox.xMax", json));
                    }

                    if (node.path("boundingBox").path("xMin").isFloatingPointNumber()) {
                        boundingBoxBuilder.setXMin(node.path("boundingBox").path("xMin").doubleValue());
                    } else {
                        throw new Exception(PnIDResponseParser.buildErrorMessage("boundingBox.xMin", json));
                    }

                    if (node.path("boundingBox").path("yMax").isFloatingPointNumber()) {
                        boundingBoxBuilder.setYMax(node.path("boundingBox").path("yMax").doubleValue());
                    } else {
                        throw new Exception(PnIDResponseParser.buildErrorMessage("boundingBox.yMax", json));
                    }

                    if (node.path("boundingBox").path("yMin").isFloatingPointNumber()) {
                        boundingBoxBuilder.setYMin(node.path("boundingBox").path("yMin").doubleValue());
                    } else {
                        throw new Exception(PnIDResponseParser.buildErrorMessage("boundingBox.xMin", json));
                    }

                    annotationBuilder.setBoundingBox(boundingBoxBuilder);
                }

                if (node.path("text").isTextual()) {
                    annotationBuilder.setText(StringValue.of(node.get("text").textValue()));
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
                if (node.path("confidence").isFloatingPointNumber()) {
                    annotationBuilder.setConfidence(DoubleValue.of(node.get("confidence").doubleValue()));
                }
                if (node.path("type").isTextual()) {
                    annotationBuilder.setType(StringValue.of(node.get("type").textValue()));
                }

                pnIDBuilder.addItems(annotationBuilder);
            }
        } else {
            throw new Exception(PnIDResponseParser.buildErrorMessage("items", json));
        }

        return pnIDBuilder.build();
    }

    /**
     * Parses a P&ID convert response json string to {@link PnIDResponse} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ConvertResponse ParsePnIDConvertResponse(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        ConvertResponse.Builder convertBuilder = ConvertResponse.newBuilder();

        if (root.path("svgUrl").isTextual()) {
            convertBuilder.setSvgUrl(StringValue.of(root.get("svgUrl").textValue()));
        } else {
            throw new Exception(PnIDResponseParser.buildErrorMessage("svgURL", json));
        }

        if (root.path("pngUrl").isTextual()) {
            convertBuilder.setPngUrl(StringValue.of(root.get("pngUrl").textValue()));
        }

        if (root.path("fileId").isIntegralNumber()) {
            convertBuilder.setFileId(Int64Value.of(root.get("fileId").longValue()));
        }
        if (root.path("fileExternalId").isTextual()) {
            convertBuilder.setFileExternalId(StringValue.of(root.get("fileExternalId").textValue()));
        }

        return convertBuilder.build();
    }

    private static String buildErrorMessage(String fieldName, String inputElement) {
        return logPrefix + "Unable to parse attribute: "+ fieldName + ". Item payload: "
                + inputElement.substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH));
    }
}
