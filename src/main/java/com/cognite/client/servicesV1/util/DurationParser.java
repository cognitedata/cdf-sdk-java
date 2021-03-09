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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class helps parse duration (on the timeline) from the text-based Cognite api specification.
 *
 * For example, the api allows duration to be specified as "2d" (2 days), "1h-ago" (1 hour ago) etc. This class will
 * translate such text into <code>Duration</code>
 */
@AutoValue
public abstract class DurationParser {
    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60L * SECOND_MS;
    private static final long HOUR_MS = 60L * MINUTE_MS;
    private static final long DAY_MS = 24L * HOUR_MS;
    private static final long WEEK_MS = 7L * DAY_MS;

    private static final String NOW = "now";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ImmutableMap<String, Long> durationMap = ImmutableMap.<String, Long>builder()
            .put("week", WEEK_MS)
            .put("w", WEEK_MS)
            .put("day", DAY_MS)
            .put("d", DAY_MS)
            .put("hour", HOUR_MS)
            .put("h", HOUR_MS)
            .put("minute", MINUTE_MS)
            .put("m", MINUTE_MS)
            .put("second", SECOND_MS)
            .put("s", SECOND_MS)
            .build();

    // Regex to parse "start/end" and "granularity".
    private final Pattern relativeTimePattern = Pattern.compile("^(\\d+)([smhdw])-ago$");
    private final Pattern granularityPattern = Pattern.compile("^(\\d*)([smhd]|second|minute|hour|day)$");

    public static Builder builder() {
        return new AutoValue_DurationParser.Builder();
    }

    public abstract Builder toBuilder();

    public Duration parseDuration(String s) throws DateTimeException {
        Preconditions.checkNotNull(s, "Input string cannot be null");
        Preconditions.checkArgument(!s.isEmpty(), "Input string cannot be empty.");
        LOG.debug("Trying to parse duration specified by string: {}", s);

        int multiplier;
        long base;

        // Check the special case of no duration (i.e. current time)
        if (NOW.equals(s)) {
            LOG.debug("String matched _now_, returning Duration.ZERO");
            return Duration.ZERO;
        }

        // Try to match with the regEx patterns
        Matcher relativeTimeMatcher = relativeTimePattern.matcher(s);
        Matcher granularityMatcher = granularityPattern.matcher(s);
        if (relativeTimeMatcher.find()) {
            LOG.debug("String matched relative time pattern.");
            multiplier = Integer.parseInt(relativeTimeMatcher.group(1));
            // Shouldn't be possible to not match a key in the map, but just checking in any case.
            if (!durationMap.containsKey(relativeTimeMatcher.group(2))) {
                LOG.error("Unable to parse duration string. Unable to find match for string: "
                        + relativeTimeMatcher.group(2));
                throw new DateTimeException("Unable to parse duration string. Unable to find match for string: "
                        + relativeTimeMatcher.group(2));
            }
            base = durationMap.get(relativeTimeMatcher.group(2));

            return Duration.ofMillis(multiplier * base);
        } else if(granularityMatcher.find()) {
            LOG.debug("String matched granularity pattern.");
            // If no multiplier is specified, set it to 1.
            if (granularityMatcher.group(1).isEmpty()) {
                multiplier = 1;
            } else {
                multiplier = Integer.parseInt(granularityMatcher.group(1));
            }

            // Shouldn't be possible to not match a key in the map, but just checking in any case.
            if (!durationMap.containsKey(granularityMatcher.group(2))) {
                LOG.error("Unable to parse duration string. Unable to find match for string: "
                        + granularityMatcher.group(2));
                throw new DateTimeException("Unable to parse duration string. Unable to find match for string: "
                        + granularityMatcher.group(2));
            }
            base = durationMap.get(granularityMatcher.group(2));

            return Duration.ofMillis(multiplier * base);
        }

        // No match, throw exception
        LOG.error("Unable to parse duration string: {}", s);
        throw new DateTimeException("Cannot parse string to duration: " + s );
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract DurationParser build();
    }
}
