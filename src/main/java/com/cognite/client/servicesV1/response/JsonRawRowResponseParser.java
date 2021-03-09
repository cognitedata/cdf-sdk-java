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

/**
 * Parses responses containing one or more raw rows. This class contains logic to detect if the items are wrapped in
 * an items array or if a single item is placed at the root.
 *
 * In the case of querying for a single row (by id/row key), only a single item is returned at the json root.
 */
@AutoValue
public abstract class JsonRawRowResponseParser extends DefaultResponseParser {

    public static Builder builder() {
        return new AutoValue_JsonRawRowResponseParser.Builder();
    }

    public abstract Builder toBuilder();


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

        JsonNode node = objectMapper.readTree(json).path("items");
        if (node.isArray()) {
            LOG.debug("Found items array in json response payload.");
            for (JsonNode child : node) {
                if (child.isObject()) {
                    tempList.add(child.toString());
                } else {
                    tempList.add(child.textValue());
                }
            }
            return ImmutableList.copyOf(tempList);
        }

        // the payload is not an items list. Check if it is a single row item.
        node = objectMapper.readTree(json);
        if (node.isObject() && node.has("key") && node.has("columns")) {
            LOG.debug("Single raw item found in json response payload.");
            tempList.add(node.toString());
            return ImmutableList.copyOf(tempList);
        }

        LOG.info("items array not found in Json payload: \r\n" + json
                .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
        return ImmutableList.of();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract JsonRawRowResponseParser build();
    }
}
