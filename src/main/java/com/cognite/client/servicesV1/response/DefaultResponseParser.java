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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;

/**
 * The default response parser extracts results items from the {@code items} node and
 * next cursor from the {@code nextCursor} node.
 */
public abstract class DefaultResponseParser implements ResponseParser<String>, Serializable {
    protected static final int MAX_LENGTH_JSON_LOG = 900;

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final String instanceId = RandomStringUtils.randomAlphanumeric(4);

    /**
     * Extract the next cursor from a response body.
     *
     * @param payload The reponse body
     * @return
     * @throws Exception
     */
    public Optional<String> extractNextCursor(byte[] payload) throws Exception {
        return extractNextCursor(parseToString(payload));
    }

    /**
     * Extract the next cursor from a results json payload.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    public Optional<String> extractNextCursor(String json) throws Exception {
        String loggingPrefix = "extractNextCursor - " + instanceId + " - ";
        JsonNode node = objectMapper.readTree(json).path("nextCursor");
        if (node.isTextual()) {
            LOG.debug(loggingPrefix + "Next cursor found: {}", node.textValue());
            return Optional.of(node.textValue());
        } else {
            LOG.debug(loggingPrefix + "Next cursor not found in Json payload.");
            LOG.trace(loggingPrefix + "Json payload: \r\n" + json
                    .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
            return Optional.empty();
        }
    }

    /**
     * Extract the results items from a response body.
     *
     * @param payload The reponse body
     * @return
     * @throws Exception
     */
    public ImmutableList<String> extractItems(byte[] payload) throws Exception {
        return extractItems(parseToString(payload));
    }

    /**
     * Extract the main items from a results json payload.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    public ImmutableList<String> extractItems(String json) throws Exception {
        String loggingPrefix = "extractItems - " + instanceId + " - ";
        ArrayList<String> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("items");
        if (!node.isArray()) {
            LOG.info(loggingPrefix + "items not found in Json payload.");
            LOG.trace(loggingPrefix + "Json payload: \r\n" + json
                    .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
            return ImmutableList.of();
        }
        for (JsonNode child : node) {
            if (child.isObject()) {
                tempList.add(child.toString());
            } else if (child.isTextual()) {
                tempList.add(child.textValue());
            } else {
                tempList.add(child.asText(""));
            }
        }
        return ImmutableList.copyOf(tempList);
    }

    protected String parseToString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
