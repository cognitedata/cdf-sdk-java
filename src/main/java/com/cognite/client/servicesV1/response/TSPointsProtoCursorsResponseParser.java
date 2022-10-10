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
import com.cognite.v1.timeseries.proto.*;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@AutoValue
public abstract class TSPointsProtoCursorsResponseParser implements ResponseParser<DataPointListItem> {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    // Logger identifier per instance
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);

    private final ObjectWriter objectWriter = JsonUtil.getObjectMapperInstance().writer();

    public static TSPointsProtoCursorsResponseParser.Builder builder() {
        return new AutoValue_TSPointsProtoCursorsResponseParser.Builder()
                .setRequest(Request.create());
    }

    public abstract TSPointsProtoCursorsResponseParser.Builder toBuilder();

    public abstract Request getRequest();

    public TSPointsProtoCursorsResponseParser withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null");
        return toBuilder().setRequest(parameters).build();
    }

    /**
     * Extracts the nextCursor for the next iteration of timestamp points.
     *
     * The cursor is a json object with each TS *externalId* (or id, if no externalId exists) mapped to the
     * nextCursor value.
     *
     * @param payload The response body
     * @return
     * @throws Exception
     */
    public Optional<String> extractNextCursor(byte[] payload) throws Exception {
        final String loggingPrefix = "Extracting next cursor [" + randomIdString + "] -";

        LOG.debug(loggingPrefix + "Start extracting next cursor from TS data points payload.");
        LOG.debug(loggingPrefix + "start parsing binary payload.");
        DataPointListResponseAlpha response = DataPointListResponseAlpha.parseFrom(payload);
        LOG.debug(loggingPrefix + "done parsing binary payload.");
        List<DataPointListItemAlpha> responseItems = response.getItemsList();

        if (responseItems.isEmpty()) {
            // No items to parse, so no cursor
            LOG.debug(loggingPrefix + "Could not find any TS list items in the response payload.");
            return Optional.empty();
        }
        LOG.debug(loggingPrefix + "Found {} TS list response items in the response payload", responseItems.size());

        List<Map<String, Object>> cursorList = new ArrayList<>();
        LOG.debug(loggingPrefix + "start iterating over all TS list response items.");
        for (DataPointListItemAlpha item : responseItems) {
            Map<String, Object> cursorItem = new HashMap<>();
            cursorItem.put("externalId", item.getExternalId());
            cursorItem.put("id", item.getId());

            if (!item.getNextCursor().isBlank()) {
                cursorItem.put("nextCursor", item.getNextCursor());
                cursorList.add(cursorItem);
            }
        }

        if (cursorList.isEmpty()) {
            // we have gotten all points, return that there is no nextCursor
            LOG.debug(loggingPrefix + "All TS in payload have completed their download. No next cursor.");
            return Optional.empty();
        }
        String cursorString = objectWriter.writeValueAsString(cursorList);
        LOG.debug(loggingPrefix + "Cursor: \r\n {}", cursorString);
        return Optional.of(cursorString);
    }


    /**
     * Extract the results items from a response body.
     *
     * @param payload The reponse body
     * @return
     * @throws Exception
     */
    public ImmutableList<DataPointListItem> extractItems(byte[] payload) throws Exception {
        final String loggingPrefix = "Extract TS items from proto payload [" + randomIdString + "] -";
        LOG.debug(loggingPrefix + "Extracting items from TS data points proto payload.");
        DataPointListResponse response = DataPointListResponse.parseFrom(payload);
        List<DataPointListItem> tempList = response.getItemsList();
        LOG.debug(loggingPrefix + "Extracting items - done parsing binary payload. Received {} TS list items", tempList.size());

        if (tempList.isEmpty()) {
            LOG.info(loggingPrefix + "No items found in the results payload.");
        }

        return ImmutableList.<DataPointListItem>copyOf(tempList);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract TSPointsProtoCursorsResponseParser.Builder setRequest(Request value);

        public abstract TSPointsProtoCursorsResponseParser build();
    }
}
