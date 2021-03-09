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

package com.cognite.client.servicesV1.response;

import com.cognite.client.Request;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Parses a Json payload and generates a {@link Request} object containing the extracted parameters.
 *
 * The parser will look for Json parameters that match the attribute map. The parameters
 * are added to the {@code RequestParameters} object as root parameters.
 *
 * The attribute map defines {@code json node} -> {@code RequestParameters root node}. The {@code key} specifies which
 * json node to extract, and the corresponding {@code value} specifies the {@link Request} root parameter name.
 * Example:
 * {@code {"jobId", "id"}} will extract the json value from {@code root.jobId} and maps it to the {@link Request}
 * parameter "id".
 *
 * If you specify '*' as a key in the attribute map, all root attributes that are value nodes (i.e. not container
 * nodes) will be added to the {@link Request}.
 *
 * A cursor is never returned from this parser.
 */
@AutoValue
public abstract class RequestParametersResponseParser implements ResponseParser<Request>, Serializable {
    private static final int MAX_LENGTH_JSON_LOG = 500;

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String instanceId = RandomStringUtils.randomAlphanumeric(6);

    static Builder builder() {
        return new AutoValue_RequestParametersResponseParser.Builder();
    }

    public static RequestParametersResponseParser of(Map<String, String> attributeMap) {
        return RequestParametersResponseParser.builder()
                .setAttributeMap(attributeMap)
                .setItemMode(false)
                .build();
    }

    abstract Builder toBuilder();

    abstract Map<String, String> getAttributeMap();
    abstract boolean isItemMode();

    /**
     * Enables/disables item mode.
     *
     * In item mode, the {@link Request} will be populated with values within an items array. This is a
     * common request pattern for performing per-item requests towards the Cognite API.
     *
     * Example:
     * Given the attribute map {@code {"jobId", "id"}}.
     * In non-item mode this will extract the json value
     * from {@code root.jobId} and map it to the {@link Request} parameter {@code root.id}.
     *
     * In item mode, this will extract the json value from {@code root.jobId} and map it to {@code root.items[n].id}
     *
     * Default is {@code false}.
     * @param enable
     * @return
     */
    public RequestParametersResponseParser enableItemMode(boolean enable) {
        return toBuilder().setItemMode(enable).build();
    }

    /**
     * Extract the next cursor from a response body.
     *
     * @param payload The response body
     * @return
     * @throws Exception
     */
    public Optional<String> extractNextCursor(byte[] payload) throws Exception {
        return Optional.empty();
    }

    /**
     * Extract the main items from a results json payload. Returns the entire payload as a json string.
     *
     * @param payload The results byte payload
     * @return
     * @throws IOException
     */
    public ImmutableList<Request> extractItems(byte[] payload) throws Exception {
        String loggingPrefix = "RequestParametersResponseParser.extractItems - " + instanceId + " - ";
        String json = parseToString(payload);
        Request requestParameters = Request.create();

        LOG.debug(loggingPrefix + "Parsing Json payload: \r\n" + json
                .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
        LOG.debug(loggingPrefix + "Start extracting {} parameters", getAttributeMap().size());
        // Check if the input string is valid json. Will throw an exception if it cannot be parsed.
        objectMapper.readTree(json);

        if (isItemMode()) {
            Map<String, Object> item = new HashMap<>();
            for (Map.Entry<String, String> entry : getAttributeMap().entrySet()) {
                if (entry.getKey().equalsIgnoreCase("*")) {
                    item = extractAllRootNodes(json, item);
                } else {
                    item = extractNode(json, entry.getKey(), item, entry.getValue());
                }
            }
            requestParameters = requestParameters
                    .withItems(ImmutableList.of(item));
        } else {
            for (Map.Entry<String, String> entry : getAttributeMap().entrySet()) {
                if (entry.getKey().equalsIgnoreCase("*")) {
                    requestParameters = extractAllRootNodes(json, requestParameters);
                } else {
                    requestParameters = extractNode(json, entry.getKey(), requestParameters, entry.getValue());
                }
            }
        }

        LOG.debug(loggingPrefix + "Built request parameters: {}", requestParameters);
        return ImmutableList.of(requestParameters);
    }

    /*
    Extracts all root nodes and puts them on the root of the {@link RequestParameters}.
     */
    private Request extractAllRootNodes(String json,
                                        Request requestParameters) throws Exception {
        Request returnValue = requestParameters;

        for (Iterator<String> it = objectMapper.readTree(json).fieldNames(); it.hasNext(); ) {
            String nodeName = it.next();
            returnValue = extractNode(json, nodeName, returnValue, nodeName);
        }

        return returnValue;
    }

    /*
    Extracts all root nodes and puts them on a Map. Will be a building block for item based requests.
     */
    private Map<String, Object> extractAllRootNodes(String json,
                                                    Map<String, Object> map) throws Exception {

        for (Iterator<String> it = objectMapper.readTree(json).fieldNames(); it.hasNext(); ) {
            String nodeName = it.next();
            extractNode(json, nodeName, map, nodeName);
        }

        return map;
    }

    /*
    Extract the value from the specified json node and add it as a parameter to the RequestParameters.
     */
    private Request extractNode(String json,
                                String path,
                                Request requestParameters,
                                String parameterName) throws Exception {
        JsonNode node = objectMapper.readTree(json).path(path);

        if (node.isIntegralNumber()) {
            return requestParameters.withRootParameter(parameterName, node.longValue());
        } else if (node.isFloatingPointNumber()) {
            return requestParameters.withRootParameter(parameterName, node.doubleValue());
        } else if (node.isBoolean()) {
            return requestParameters.withRootParameter(parameterName, node.booleanValue());
        } else if (node.isTextual()) {
            return requestParameters.withRootParameter(parameterName, node.textValue());
        } else {
            return requestParameters;
        }
    }

    /*
    Extract the value from the specified json node and add it as a parameter to the map.
    This supports building an item with the required parameters (i.e. item mode).
     */
    private Map<String, Object> extractNode(String json,
                                            String path,
                                            Map<String, Object> map,
                                            String parameterName) throws Exception {
        JsonNode node = objectMapper.readTree(json).path(path);

        if (node.isIntegralNumber()) {
            map.put(parameterName, node.longValue());
        } else if (node.isFloatingPointNumber()) {
            map.put(parameterName, node.doubleValue());
        } else if (node.isBoolean()) {
            map.put(parameterName, node.booleanValue());
        } else if (node.isTextual()) {
            map.put(parameterName, node.textValue());
        }

        return map;
    }

    private String parseToString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setAttributeMap(Map<String, String> value);
        abstract Builder setItemMode(boolean value);

        public abstract RequestParametersResponseParser build();
    }
}
