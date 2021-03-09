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
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Optional;

/**
 * Parses a single attribute from a json payload as a single item.
 *
 * A cursor is never returned from this parser.
 */
@AutoValue
public abstract class JsonStringAttributeResponseParser extends DefaultResponseParser {
    private static String DEFAULT_ATTRIBUTE_PATH = "";

    static Builder builder() {
        return new AutoValue_JsonStringAttributeResponseParser.Builder();
    }

    public static JsonStringAttributeResponseParser create() {
        return JsonStringAttributeResponseParser.builder()
                .setAttributePath(DEFAULT_ATTRIBUTE_PATH)
                .build();
    }

    public abstract JsonStringAttributeResponseParser.Builder toBuilder();
    public abstract String getAttributePath();

    /**
     * Sets the path to the attribute to extract.
     *
     * @param path
     * @return
     */
    public JsonStringAttributeResponseParser withAttributePath(String path) {
        return toBuilder().setAttributePath(path).build();
    }

    /**
     * Extract the next cursor from a results json payload. Will always return an empty Optional.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    @Override
    public Optional<String> extractNextCursor(String json) {
        return Optional.empty();
    }

    /**
     * Extract the main items from a results json payload. Returns the entire payload as a json string.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    public ImmutableList<String> extractItems(String json) throws Exception {
        String loggingPrefix = "extractItems - " + instanceId + " - ";
        // Check if the input string is valid json. Will throw an exception if it cannot be parsed.
        JsonNode node = objectMapper.readTree(json).path(getAttributePath());

        if (node.isMissingNode()) {
            LOG.warn(loggingPrefix + "Cannot find any attribute at path [{}]", getAttributePath());
            return ImmutableList.of();
        } else if (node.isTextual()) {
            return ImmutableList.of(node.textValue());
        } else if (node.isValueNode()) {
            return ImmutableList.of(node.asText(""));
        } else {
            return ImmutableList.of(node.toString());
        }
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setAttributePath(String value);

        public abstract JsonStringAttributeResponseParser build();
    }
}
