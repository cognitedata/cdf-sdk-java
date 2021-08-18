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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import okhttp3.Response;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This is a helper class for transporting a http response. We cannot use the <code>okhttp3.Response</code> object
 * as its main payload can only be consumed once and must be closed as soon as possible to prevent holding on to
 * connection resources.
 *
 * This class will hold both the binary response payload as well as its metadata.
 */
@AutoValue
public abstract class ResponseBinary implements Serializable {

    static Builder builder() {
        return new AutoValue_ResponseBinary.Builder()
                .setApiRetryCounter(0);
    }

    public static ResponseBinary of(Response response, ByteString body) {
        Preconditions.checkNotNull(response, "Response cannot be null.");

        return ResponseBinary.builder()
                .setResponse(response)
                .setResponseBodyBytes(body)
                .build();
    }

    abstract Builder toBuilder();

    /**
     * Returns the original {@link Response} (less the body) with metadata and header values.
     *
     * @return The response metadata
     */
    public abstract Response getResponse();

    /**
     * Returns the main body of the response.
     *
     * @return The main body of the response.
     */
    public abstract ByteString getResponseBodyBytes();
    public abstract int getApiRetryCounter();

    /**
     * Returns the measured latency in getting the first response from the api. The latency is calculated by
     * measuring the duration between sending the request to the api and receiving (but not processing) the
     * response.
     *
     * @return The measured api latency in milliseconds for producing this response.
     */
    @Nullable
    public abstract Long getApiLatency();

    /**
     * Returns the measured queue duration of an async api request. These requests typically represent
     * potentially long-running jobs like contextualization tasks (entity matching, P&ID parsing, etc.).
     *
     * Async api requests first enter a job queue before they are processed by a worker. If the queue duration
     * grows over time, it is a sign that the api is unable to scale up to meet the request demand.
     *
     * @return The job queue duration in milliseconds.
     */
    @Nullable
    public abstract Long getApiJobQueueDurationMillies();

    /**
     * Returns the measured job duration of an async api request. These requests typically represent
     * potentially long-running jobs like contextualization tasks (entity matching, P&ID parsing, etc.).
     *
     * @return The job duration in milliseconds.
     */
    @Nullable
    public abstract Long getApiJobDurationMillies();

    public ResponseBinary withApiRetryCounter(int count) {
        return toBuilder().setApiRetryCounter(count).build();
    }

    public ResponseBinary withApiLatency(long latency) {
        return toBuilder().setApiLatency(latency).build();
    }


    /**
     * Set the api job queue duration (in milliseconds) associated with this response. Only applicable for
     * async api calls.
     *
     * @param millies the api queue duration in milliseconds.
     * @return The ResponseBinary with the queue duration.
     */
    public ResponseBinary withApiJobQueueDuration(long millies) {
        return toBuilder().setApiJobQueueDurationMillies(millies).build();
    }

    /**
     * Set the api job duration (in milliseconds) associated with this response. Only applicable for
     * async api calls.
     *
     * @param millies the api job duration in milliseconds.
     * @return The ResponseBinary with the job duration.
     */
    public ResponseBinary withApiJobDuration(long millies) {
        return toBuilder().setApiJobDurationMillies(millies).build();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setResponse(Response value);
        abstract Builder setResponseBodyBytes(ByteString value);
        abstract Builder setApiRetryCounter(int value);
        abstract Builder setApiLatency(Long value);
        abstract Builder setApiJobQueueDurationMillies(Long value);
        abstract Builder setApiJobDurationMillies(Long value);

        abstract ResponseBinary build();
    }
}
