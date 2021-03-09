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
import com.cognite.client.servicesV1.util.DurationParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

@AutoValue
public abstract class TSPointsResponseParser extends DefaultResponseParser {
    private static final int DEFAULT_PARAMETER_LIMIT = 100;

    private static final String END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";

    private final DurationParser durationParser = DurationParser.builder().build();

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

        JsonNode dataitems = objectMapper.readTree(json).path("items");
        if (dataitems.isArray()) {
            LOG.debug("Extracting next cursor. Found items in Json array.");
            JsonNode datapoints = dataitems.get(0).path("datapoints");
            if (datapoints.isArray()) {
                LOG.debug("Extracting next cursor. Found datapoints in items Json array.");
                // first check the length of the json response to see if we received *limit* number of points
                final int limit = (Integer) getRequest().getRequestParameters()
                                .getOrDefault("limit", DEFAULT_PARAMETER_LIMIT);
                LOG.debug("Extracting next cursor. Limit parameter in request: {}.", limit);
                if (datapoints.size() >= limit) {
                    LOG.debug("Extracting next cursor. Limit parameter and datapoints size are equal.");
                    // get the time of the last datapoint
                    JsonNode lastPoint = datapoints.get(datapoints.size() - 1);
                    long lastTimestamp = lastPoint.get("timestamp").longValue();
                    LOG.debug("Extracting next cursor. Last datapoint timestamp: {}.", lastTimestamp);
                    long endTimestamp = Instant.now().toEpochMilli();
                    // check if we have an end time (otherwise it was now)
                    if (getRequest().getRequestParameters().containsKey(END_KEY)) {
                        LOG.debug("Extracting next cursor. Request contains end key: {}",
                                getRequest().getRequestParameters().containsKey(END_KEY));

                        // if end is String, we need to parse it
                        if (getRequest().getRequestParameters().get(END_KEY) instanceof String) {
                            LOG.debug("Extracting next cursor. Trying to parse end key");
                            endTimestamp = System.currentTimeMillis() - durationParser
                                    .parseDuration((String) getRequest().getRequestParameters().get(END_KEY))
                                    .toMillis();

                        } else if (getRequest().getRequestParameters().get(END_KEY) instanceof Number) {
                            LOG.debug("Extracting next cursor. End key matched epoch timestamp");
                            endTimestamp = (Long) getRequest().getRequestParameters().get(END_KEY);
                        } else {
                            // no compatible type.
                            LOG.error("Parameter end is not a compatible type: "
                                    + getRequest().getRequestParameters().get(END_KEY).getClass().getCanonicalName());
                            throw new Exception("Parameter end is not a compatible type: "
                                    + getRequest().getRequestParameters().get(END_KEY).getClass().getCanonicalName());
                        }
                    }
                    // if we do have an end time, check that the latest point is not past it
                    if (lastTimestamp + 1 >= endTimestamp) {
                        LOG.debug("Extracting next cursor. Last timestamp > end key. No next cursor.");
                        // we have gotten all points, return that there is no nextCursor
                        return Optional.empty();
                    }

                    // we are missing datapoints, return the next expected Timestamp
                    LOG.debug("Extracting next cursor. Need to fetch more datapoints. Building next cursor.");
                    long nextDelta = 1;

                    // Check if this is an aggregation
                    if (getRequest().getRequestParameters().containsKey(GRANULARITY_KEY)) {
                        LOG.debug("Extracting next cursor. Request is an aggregation request: {}",
                                getRequest().getRequestParameters().get(GRANULARITY_KEY));
                        // Parse the granularity specification
                        if (getRequest().getRequestParameters().get(GRANULARITY_KEY) instanceof String) {
                            LOG.debug("Extracting next cursor. Trying to parse the granularity key.");
                            String granularityString = (String) getRequest().getRequestParameters().get(GRANULARITY_KEY);
                            nextDelta = durationParser.parseDuration(granularityString).toMillis();

                        } else {
                            // no compatible type.
                            LOG.error("Parameter " + GRANULARITY_KEY + " is not a compatible type: "
                                    + getRequest().getRequestParameters().get(GRANULARITY_KEY)
                                            .getClass().getCanonicalName());
                            throw new Exception("Parameter " + GRANULARITY_KEY + " is not a compatible type: "
                                    + getRequest().getRequestParameters().get(GRANULARITY_KEY)
                                    .getClass().getCanonicalName());
                        }
                    }

                    return Optional.of(Long.toString(lastTimestamp + nextDelta));
                }
            }
        }
        // we have gotten all points, return that there is no nextCursor
        LOG.info("Next cursor not found in Json payload: \r\n" + json
                .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
        return Optional.empty();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract TSPointsResponseParser.Builder setRequest(Request value);

        public abstract TSPointsResponseParser build();
    }
}
