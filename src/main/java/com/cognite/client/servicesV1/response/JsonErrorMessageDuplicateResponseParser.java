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
 * Extracts a list of items from the error sub-path in a json payload.
 */
@AutoValue
public abstract class JsonErrorMessageDuplicateResponseParser extends DefaultResponseParser {

    public static Builder builder() {
        return new AutoValue_JsonErrorMessageDuplicateResponseParser.Builder()
                .setErrorSubPath("message");
    }

    public abstract Builder toBuilder();
    public abstract String getErrorSubPath();

    /**
     * Extract the main items from a results json payload.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    @Override
    public ImmutableList<String> extractItems(String json) throws Exception {
        ArrayList<String> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("error").path(getErrorSubPath());
        if (!node.isTextual()) {
            LOG.info("items not found in Json payload: \r\n" + json
                    .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
            return ImmutableList.of();
        }
        String errorMessage = node.asText();
        if (errorMessage.startsWith("The following external id(s) are already in the database:")) {
            String externalIdString = errorMessage.substring(58);
            LOG.debug("Identified duplicates string: " + externalIdString);
            for (String item : externalIdString.split(", ")) {
                tempList.add(new StringBuilder()
                        .append("{\"externalId\":\"")
                        .append(item)
                        .append("\"}")
                        .toString());
            }
        }

        return ImmutableList.copyOf(tempList);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setErrorSubPath(String value);

        public abstract JsonErrorMessageDuplicateResponseParser build();
    }
}
