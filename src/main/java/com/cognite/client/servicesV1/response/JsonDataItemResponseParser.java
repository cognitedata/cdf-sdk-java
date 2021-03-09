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
import java.util.ArrayList;
import java.util.Optional;

/**
 * Extracts results items from a json payload corresponding to the api v0.6 specification.
 */
@AutoValue
public abstract class JsonDataItemResponseParser extends DefaultResponseParser {

    public static Builder builder() {
        return new com.cognite.client.servicesV1.response.AutoValue_JsonDataItemResponseParser.Builder();
    }

    public abstract Builder toBuilder();

    /**
     * Extract the next cursor from a results json payload.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    @Override
    public Optional<String> extractNextCursor(String json) throws IOException {
        JsonNode node = objectMapper.readTree(json).path("data").path("nextCursor");
        if (node.isTextual()) {
            return Optional.of(node.textValue());
        } else {
            LOG.info("Next cursor not found in Json payload: \r\n" + json
                    .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
            return Optional.empty();
        }
    }

    /**
     * Extract the main items from a results json payload corresponding to v0.6 spec.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    @Override
    public ImmutableList<String> extractItems(String json) throws Exception {
        ArrayList<String> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("data").path("items");
        if (!node.isArray()) {
            LOG.info("items not found in Json payload: \r\n" + json
                    .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
            return ImmutableList.of();
        }
        for (JsonNode child : node) {
            if (child.isObject()) {
                tempList.add(child.toString());
            } else {
                tempList.add(child.textValue());
            }
        }
        return ImmutableList.copyOf(tempList);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract JsonDataItemResponseParser build();
    }
}
