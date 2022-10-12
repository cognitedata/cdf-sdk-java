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
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.*;

@AutoValue
public abstract class TSPointsResponseParser extends DefaultResponseParser {
    private static final ObjectWriter objectWriter = JsonUtil.getObjectMapperInstance().writer();

    public static TSPointsResponseParser.Builder builder() {
        return new com.cognite.client.servicesV1.response.AutoValue_TSPointsResponseParser.Builder()
                .setRequest(Request.create());
    }

    public abstract TSPointsResponseParser.Builder toBuilder();

    public abstract Request getRequest();

    public TSPointsResponseParser withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null");

        return toBuilder().setRequest(parameters).build();
    }

    /**
     * Extract the next cursor from a results json payload.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    @Override
    public Optional<String> extractNextCursor(String json) throws Exception {
        LOG.info("Extracting next cursor from TS datapoints payload.");

        List<Map<String, Object>> cursorList = new ArrayList<>();
        JsonNode dataitems = JsonUtil.getObjectMapperInstance().readTree(json).path("items");
        if (dataitems.isArray()) {
            LOG.debug("Start iterating over all TS list response items.");
            for (JsonNode item : dataitems) {
                if (item.path("nextCursor").isTextual()) {
                    Map<String, Object> cursorItem = new HashMap<>();
                    cursorItem.put("nextCursor", item.path("nextCursor").textValue());
                    if (item.path("externalId").isTextual())
                        cursorItem.put("externalId", item.path("externalId").textValue());
                    if (item.path("id").isIntegralNumber())
                        cursorItem.put("id", item.path("id").longValue());
                    cursorList.add(cursorItem);
                }
            }
        }

        if (cursorList.isEmpty()) {
            // we have gotten all points, return that there is no nextCursor
            LOG.debug("All TS in payload have completed their download. No next cursor.");
            return Optional.empty();
        }
        String cursorString = objectWriter.writeValueAsString(cursorList);
        LOG.debug("Cursor: \r\n {}", cursorString);
        return Optional.of(cursorString);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract TSPointsResponseParser.Builder setRequest(Request value);

        public abstract TSPointsResponseParser build();
    }
}
