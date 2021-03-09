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
import com.cognite.client.servicesV1.util.TSIterationUtilities;
import com.cognite.v1.timeseries.proto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@AutoValue
public abstract class TSPointsProtoResponseParser implements ResponseParser<DataPointListItem> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int DEFAULT_PARAMETER_LIMIT = 10000;

    private static final String END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    // Logger identifier per instance
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);

    private final ObjectWriter objectWriter = mapper.writer();

    public static TSPointsProtoResponseParser.Builder builder() {
        return new com.cognite.client.servicesV1.response.AutoValue_TSPointsProtoResponseParser.Builder()
                .setRequest(Request.create());
    }

    public abstract TSPointsProtoResponseParser.Builder toBuilder();

    public abstract Request getRequest();

    public TSPointsProtoResponseParser withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null");
        return toBuilder().setRequest(parameters).build();
    }

    /**
     * Extracts the start timestamp for the next iteration of timestamp points.
     *
     * The cursor is a json object with each TS *externalId* (or id, if no externalId exists) mapped to the next
     * *start* timestamp.
     *
     * @param payload The response body
     * @return
     * @throws Exception
     */
    public Optional<String> extractNextCursor(byte[] payload) throws Exception {
        final String loggingPrefix = "Extracting next cursor [" + randomIdString + "] -";

        LOG.debug(loggingPrefix + "Start extracting next cursor from TS data points payload.");
        LOG.debug(loggingPrefix + "start parsing binary payload.");
        DataPointListResponse response = DataPointListResponse.parseFrom(payload);
        LOG.debug(loggingPrefix + "done parsing binary payload.");
        List<DataPointListItem> responseItems = response.getItemsList();

        if (responseItems.isEmpty()) {
            // No items to parse, so no cursor
            LOG.debug(loggingPrefix + "Could not find any TS list items in the response payload.");
            return Optional.empty();
        }
        LOG.debug(loggingPrefix + "Found {} TS list response items in the response payload", responseItems.size());
        // Adjust the limit based on the number of TS in the request
        int effectiveLimit = TSIterationUtilities.calculateLimit(
                (Integer) getRequest().getRequestParameters().getOrDefault("limit", DEFAULT_PARAMETER_LIMIT),
                responseItems.size());

        List<Map<String, Object>> cursorList = new ArrayList<>();
        LOG.debug(loggingPrefix + "start iterating over all TS list response items. Effective limit set to {}", effectiveLimit);
        for (DataPointListItem item : responseItems) {
            Map<String, Object> cursorItem = new HashMap<>();
            cursorItem.put("externalId", item.getExternalId());
            cursorItem.put("id", item.getId());

            OptionalLong nextTimestamp = getNextTimestampForTs(item, effectiveLimit);
            if (nextTimestamp.isPresent()) {
                cursorItem.put("timestamp", nextTimestamp.getAsLong());
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

    /**
     * Extracts the next expected timestamp for a list of timeseries points. This method is used to help iterate a
     * timeseries query by producing the _startTime_ parameter of the next iteration/query.
     *
     * Both raw datapoints and aggregates are supported, but *includeOutsidePoints* are not supported.
     *
     * If the current payload/list of datapoints represents the final set of the iteration, this method will
     * return an empty Optional.
     *
     * @param datapoints The timeseries to analyze
     * @param limit The limit setting in the request
     * @return The start timestamp for the next iteration, or an empty optional if this represents the final payload.
     * @throws Exception
     */
    private OptionalLong getNextTimestampForTs(DataPointListItem datapoints, int limit) throws Exception {
        final String loggingPrefix = "Get cursor for TS [" + randomIdString + "] -";
        List<Long> timestamps = new ArrayList<>(10000);

        LOG.debug(loggingPrefix + "extracting the list of timestamps from the TS.");
        if (datapoints.hasNumericDatapoints()) {
            List<NumericDatapoint> points = datapoints.getNumericDatapoints().getDatapointsList();
            for (NumericDatapoint point : points) {
                timestamps.add(point.getTimestamp());
            }
        } else if (datapoints.hasStringDatapoints()) {
            List<StringDatapoint> points = datapoints.getStringDatapoints().getDatapointsList();
            for (StringDatapoint point : points) {
                timestamps.add(point.getTimestamp());
            }
        } else if (datapoints.hasAggregateDatapoints()) {
            List<AggregateDatapoint> points = datapoints.getAggregateDatapoints().getDatapointsList();
            for (AggregateDatapoint point : points) {
                timestamps.add(point.getTimestamp());
            }
        }

        // First check the length of the response to see if we received *limit* number of points.
        // If we received less than *limit*, there are no more TS points to iterate over.
        LOG.debug(loggingPrefix + "Effective limit parameter for the request: {}.", limit);
        LOG.debug(loggingPrefix + "Number of TS points in the response: {}.", timestamps.size());
        if (timestamps.size() < limit) {
            return OptionalLong.empty();
        }

        // We may have more datapoints to fetch--let's investigate further.
        LOG.debug(loggingPrefix + "Limit parameter and datapoints size are equal.");
        // get the time of the last datapoint
        long lastTimestamp = timestamps.get(timestamps.size() - 1);
        LOG.debug(loggingPrefix + "Last datapoint timestamp: {}.", lastTimestamp);
        long endTimestamp = Instant.now().toEpochMilli();
        // check if we have an end time (otherwise it was now)
        LOG.debug(loggingPrefix + "Check request for end time from attribute {}: [{}]",
                END_KEY,
                getRequest().getRequestParameters().get(END_KEY));
        Optional<Long> requestEndTime = TSIterationUtilities.getEndAsMillis(getRequest());
        if (requestEndTime.isPresent()) {
            endTimestamp = requestEndTime.get();
        }

        // check that the latest point is not past the end time
        if (lastTimestamp + 1 >= endTimestamp) {
            LOG.debug(loggingPrefix + "Last timestamp > end key. No next cursor.");
            // we have gotten all points, return that there is no nextCursor
            return OptionalLong.empty();
        }

        // we are missing datapoints, return the next expected Timestamp
        LOG.debug(loggingPrefix + "Need to fetch more datapoints. Building next cursor.");
        long nextDelta = 1; // the default delta for raw datapoints

        // Check if this is an aggregation
        Optional<Duration> aggGranularity = TSIterationUtilities.getAggregateGranularityDuration(getRequest());
        if (aggGranularity.isPresent()) {
            LOG.debug(loggingPrefix + "Request is an aggregation request: {}",
                    getRequest().getRequestParameters().get(GRANULARITY_KEY));
            nextDelta = aggGranularity.get().toMillis();
        }

        // check that the new timestamp (cursor) point is not past the end time
        if (lastTimestamp + nextDelta > endTimestamp) {
            LOG.debug(loggingPrefix + "New timestamp > end key. No next cursor.");
            // we have gotten all points, return that there is no nextCursor
            return OptionalLong.empty();
        }

        return OptionalLong.of(lastTimestamp + nextDelta);
    }


    @AutoValue.Builder
    public abstract static class Builder {
        public abstract TSPointsProtoResponseParser.Builder setRequest(Request value);

        public abstract TSPointsProtoResponseParser build();
    }
}
