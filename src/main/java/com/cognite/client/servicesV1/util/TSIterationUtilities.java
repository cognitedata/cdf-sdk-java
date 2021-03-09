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

package com.cognite.client.servicesV1.util;

import com.cognite.client.Request;

import java.time.Duration;
import java.util.Optional;

/**
 * Helper class for various utility methods for helping iterating over timeseries data points requests/responses
 */
public final class TSIterationUtilities {
    private static final String START_KEY = "start";
    private static final String END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";
    private static final DurationParser durationParser = DurationParser.builder().build();

    /**
     * Calculates a limit setting for a timeseries datapoints request based on the baseline limit setting
     * and the number of TS items in the request.
     *
     * The baseline limit is the root parameter "limit" in the request specification.
     *
     * @param originalLimit
     * @param noTimeseries
     * @return
     */
    public static int calculateLimit(int originalLimit, int noTimeseries) {
        return (int) Math.max(Math.floor(originalLimit / Math.max(noTimeseries, 1)), 1);
    }

    /**
     * Parses the start attribute from a TS datapoints request and returns it as millis since epoch.
     *
     * @param requestParameters
     * @return
     * @throws Exception
     */
    public static Optional<Long> getStartAsMillis(Request requestParameters) throws Exception {
        return getTimestampFromRequest(requestParameters, START_KEY);
    }

    /**
     * Parses the end attribute from a TS datapoints request and returns it as millis since epoch.
     *
     * @param requestParameters
     * @return
     * @throws Exception
     */
    public static Optional<Long> getEndAsMillis(Request requestParameters) throws Exception {
        return getTimestampFromRequest(requestParameters, END_KEY);
    }

    /**
     * Parses the aggregate duration (minute, hour, day, etc.) from a TS datapoints request.
     *
     * @param requestParameters
     * @return
     * @throws Exception
     */
    public static Optional<Duration> getAggregateGranularityDuration(Request requestParameters) throws Exception {
        Optional<Duration> returnValue = Optional.empty();

        // Check if this is an aggregation
        if (requestParameters.getRequestParameters().containsKey(GRANULARITY_KEY)) {
            // Parse the granularity specification
            if (requestParameters.getRequestParameters().get(GRANULARITY_KEY) instanceof String) {
                String granularityString = (String) requestParameters.getRequestParameters().get(GRANULARITY_KEY);
                returnValue = Optional.of(durationParser.parseDuration(granularityString));

            } else {
                throw new Exception("Parameter " + GRANULARITY_KEY + " is not a compatible type: "
                        + requestParameters.getRequestParameters().get(GRANULARITY_KEY)
                        .getClass().getCanonicalName());
            }
        }
        return returnValue;
    }

    /**
     * Parses a timestamp expression from a request.
     * @param requestParameters
     * @param parameterKey
     * @return
     * @throws Exception
     */
    private static Optional<Long> getTimestampFromRequest(Request requestParameters,
                                                          String parameterKey) throws Exception {
        Optional<Long> returnValue = Optional.empty();

        if (requestParameters.getRequestParameters().containsKey(parameterKey)) {
            // if end is String, we need to parse it
            if (requestParameters.getRequestParameters().get(parameterKey) instanceof String) {
                returnValue = Optional.of(System.currentTimeMillis() - durationParser
                        .parseDuration((String) requestParameters.getRequestParameters().get(parameterKey))
                        .toMillis());
            } else if (requestParameters.getRequestParameters().get(parameterKey) instanceof Number) {
                returnValue = Optional.of((Long) requestParameters.getRequestParameters().get(parameterKey));
            } else {
                // no compatible type.
                String message = "Parameter *" + parameterKey + "* is not a compatible type: "
                        + requestParameters.getRequestParameters().get(parameterKey).getClass().getCanonicalName();
                throw new Exception(message);
            }
        }
        return returnValue;
    }
}
