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

package com.cognite.client.servicesV1;

import java.time.Duration;

/**
 * This class holds a set of constants used by various connector services.
 */
public final class ConnectorConstants {
    private ConnectorConstants() {}

    /*
    API request identifiers
     */
    public final static String SDK_IDENTIFIER = "cdf-sdk-java-1.1.x";
    public final static String DEFAULT_APP_IDENTIFIER = "cdf-sdk-java";
    public final static String DEFAULT_SESSION_IDENTIFIER = "cdf-sdk-java";

    /*
    Constants related to executing api http requests.
     */
    public static final boolean DEFAULT_BETA_ENABLED = false;
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int MIN_MAX_RETRIES = 1;
    public static final int MAX_MAX_RETRIES = 20;
    public static final String DEFAULT_ENDPOINT = "";
    public static final Duration DEFAULT_ASYNC_API_JOB_TIMEOUT = Duration.ofMinutes(15);
    public static final Duration DEFAULT_ASYNC_API_JOB_POLLING_INTERVAL = Duration.ofSeconds(2);

    /*
    Default batch sizes for api requests
     */
    public final static int DEFAULT_MAX_BATCH_SIZE = 1000;
    public final static int DEFAULT_MAX_BATCH_SIZE_RAW = 2000;
    public final static int DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS = 100000;
    public final static int DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS_AGG = 10000;
    public final static int DEFAULT_MAX_BATCH_SIZE_SEQUENCES_ROWS = 2000;

    public final static int DEFAULT_MAX_BATCH_SIZE_WRITE = 1000;
    public final static int DEFAULT_MAX_BATCH_SIZE_WRITE_RAW = 2000;
    public final static int DEFAULT_MAX_BATCH_SIZE_WRITE_TS_DATAPOINTS = 100000;

    public static final int DEFAULT_SEQUENCE_WRITE_MAX_ROWS_PER_ITEM = 10_000;
    public static final int DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH = 10;
    public static final int DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH = 100_000;
    public static final int DEFAULT_SEQUENCE_WRITE_MAX_CHARS_PER_BATCH = 1_000_000;

    /*
    Controls the recording of metrics
     */
    public static final boolean DEFAULT_METRICS_ENABLED = false;

    /*
    Various
     */
    public static final int MAX_LOG_ELEMENT_LENGTH = 1000;
}
