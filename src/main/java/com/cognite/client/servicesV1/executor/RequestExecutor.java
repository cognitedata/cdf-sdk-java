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

package com.cognite.client.servicesV1.executor;

import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.ResponseBinary;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class will execute an okhttp3 request on a separate thread and publish the result via a
 * <code>CompletableFuture</code>. This allows the client code to spin off multiple concurrent request without blocking
 * the main thread.
 *
 * This represents the "blocking IO on a separate thread" pattern, and will work fine for client workloads (limited
 * number of concurrent requests).
 */
@AutoValue
public abstract class RequestExecutor {
    // Valid response codes *outside* the 200-range.
    private static final ImmutableList<Integer> DEFAULT_VALID_RESPONSE_CODES = ImmutableList.of();

    private static final ImmutableList<Integer> RETRYABLE_RESPONSE_CODES = ImmutableList.of(
            408,    // request timeout
            429,    // too many requests
            500,    // internal server error
            502,    // bad gateway
            503,    // service unavailable
            504     // gateway timeout
    );

    private static final ImmutableList<Class<? extends Exception>> RETRYABLE_EXCEPTIONS = ImmutableList.of(
            IOException.class
    );

    private static final int DEFAULT_CPU_MULTIPLIER = 8;
    private static final ForkJoinPool DEFAULT_POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors()
            * DEFAULT_CPU_MULTIPLIER);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "RequestExecutor [" + randomIdString + "] -";

    private static Builder builder() {
        return new AutoValue_RequestExecutor.Builder()
                .setExecutor(DEFAULT_POOL)
                .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                .setValidResponseCodes(DEFAULT_VALID_RESPONSE_CODES);
    }

    public static RequestExecutor of(OkHttpClient client) {
        Preconditions.checkNotNull(client, "Http client cannot be null.");

        return RequestExecutor.builder()
                .setHttpClient(client)
                .build();
    }

    abstract Builder toBuilder();
    abstract OkHttpClient getHttpClient();
    abstract List<Integer> getValidResponseCodes();
    abstract Executor getExecutor();
    abstract int getMaxRetries();

    /**
     * Sets the http client to use for executing the http requests.
     *
     * @param client the http client to use for requests.
     * @return the RequestExecutor with the applied configuration.
     */
    public RequestExecutor withHttpClient(OkHttpClient client) {
        return toBuilder().setHttpClient(client).build();
    }

    /**
     * Sets the executor to use for running the api requests.
     *
     * The default executor is a <code>ForkJoinPool</code> with a target parallelism of four threads per core.
     * @param executor the executor to use for running the api requests.
     * @return the RequestExecutor with the applied configuration.
     */
    public RequestExecutor withExecutor(Executor executor) {
        Preconditions.checkNotNull(executor, "Executor cannot be null.");
        return toBuilder().setExecutor(executor).build();
    }

    /**
     * Sets the maximum number of retries.
     *
     * The default setting is 3.
     * @param retries the max number of retries
     * @return the RequestExecutor with the applied configuration.
     */
    public RequestExecutor withMaxRetries(int retries) {
        Preconditions.checkArgument(retries <= ConnectorConstants.MAX_MAX_RETRIES
                        && retries >= ConnectorConstants.MIN_MAX_RETRIES,
                "Max retries out of range. Must be between "
                            + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);
        return toBuilder().setMaxRetries(retries).build();
    }

    /**
     * Specifies a set of valid http response codes *in addition* to the 200-range.
     *
     * By default, any 2xx response is considered a valid response. By specifying additional codes, this executor
     * will return responses from outside the 200-range. This could be useful in case you want to handle non-200
     * responses with custom logic. For example, duplicate detection and constraint violations are reported
     * as non-200 responses from the Cognite API.
     *
     * @param validResponseCodes A list of valid response codes.
     * @return the RequestExecutor with the applied configuration.
     */
    public RequestExecutor withValidResponseCodes(List<Integer> validResponseCodes) {
        Preconditions.checkNotNull(validResponseCodes, "Valid response codes cannot be null.");
        return toBuilder().setValidResponseCodes(validResponseCodes).build();
    }

    /**
     * Executes a given request. Checks for transient server errors and retires the request until a valid response
     * is produced, or the max number of retries is reached. This method executes as a blocking I/O operation on a
     * separate thread--the calling thread is not blocked and can continue working on its tasks.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     * @param request The request to execute
     * @return a {@link CompletableFuture} encapsulating the future response.
     */
    public CompletableFuture<ResponseBinary> executeRequestAsync(Request request) {
        LOG.debug(loggingPrefix + "Executing request async.");

        CompletableFuture<ResponseBinary> completableFuture = new CompletableFuture<>();
        getExecutor().execute((Runnable & CompletableFuture.AsynchronousCompletionTask) () -> {
            try {
                completableFuture.complete(this.executeRequest(request));
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        });

        return completableFuture;
    }

    /**
     * Executes a given request. Checks for transient server errors and retires the request until a valid response
     * is produced, or the max number of retries is reached. This method blocks until a <code>Response</code>
     * is produced.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     * The async version of this method is <code>executeRequestAsync</code>
     *
     * */
    public ResponseBinary executeRequest(Request request) throws Exception {
        LOG.debug(loggingPrefix + "Executing request batch to [{}]", request.url().toString());
        Long apiLatency = 0L;
        int apiRetryCounter = 0;

        List<Exception> catchedExceptions = new ArrayList<>();
        int responseCode = -1;
        String requestId = "";
        Long timerStart;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // progressive back off in case of retries.
        for (int callNo = 0;
             callNo < this.getMaxRetries();
             callNo++,
                     Thread.sleep(Math.min(32000L, (500L * (long) Math.exp(callNo)) + random.nextLong(1000)))) {
            timerStart = System.currentTimeMillis();

            try (Response response = getHttpClient().newCall(request).execute()) {
                apiLatency = (System.currentTimeMillis() - timerStart);
                requestId = response.header("x-request-id");
                responseCode = response.code();
                LOG.debug(loggingPrefix + "Response received with request id {} and response code {}",
                        requestId, responseCode);

                // if the call was not successful, throw an error
                if (!response.isSuccessful() && !getValidResponseCodes().contains(responseCode)) {
                    throw new IOException(
                            "Reading from Fusion: Unexpected response code: " + responseCode + ". "
                                    + response.toString() + System.lineSeparator()
                                    + "Response body: " + response.body().string() + System.lineSeparator()
                                    + "Response headers: " + response.headers().toString() + System.lineSeparator()
                    );
                }
                // check the response
                if (response.body() == null) {
                    throw new Exception("Reading from Fusion: Successful response, but the body is null. "
                                                + response.toString() + System.lineSeparator()
                                                + "Response headers: " + response.headers().toString());
                }

                // check the content length. When downloading very large files this may exceed 4GB
                // we put a limit of 500MiB on the response
                if (response.body().contentLength() > (1024L * 1024L * 500L)) {
                    String message = String.format("Response too large. "
                            + "Content-length = [%d]. %n"
                            + "Response headers: %s",
                            response.body().contentLength(),
                            response.headers().toString());
                    throw new IOException(message);
                }

                return ResponseBinary.of(response, ByteString.readFrom(response.body().byteStream()))
                        .withApiLatency(apiLatency)
                        .withApiRetryCounter(apiRetryCounter);
            } catch (Exception e) {
                catchedExceptions.add(e);

                // if we get a transient error, retry the call
                if (RETRYABLE_EXCEPTIONS.stream().anyMatch(known -> known.isInstance(e.getClass()))
                        || RETRYABLE_RESPONSE_CODES.contains(responseCode)) {
                    apiRetryCounter++;
                    LOG.warn(loggingPrefix + "Transient error when reading from Fusion (request id: " + requestId
                            + ", response code: " + responseCode
                            + "). Retrying...", e);

                } else {
                    // not transient, just re-throw
                    LOG.error(loggingPrefix + "Non-transient error occurred when reading from Fusion. Request id: " + requestId
                            + ", Response code: " + responseCode, e);
                    throw e;
                }
            }
        }

        // No results are produced. Throw the list of registered Exception.
        String exceptionMessage = "Unable to produce a valid response from Cognite Fusion.";
        IOException e;
        if (catchedExceptions.size() > 0) { //add the details of the most recent exception.
            Exception mostRecentException = catchedExceptions.get(catchedExceptions.size() -1);
            exceptionMessage += System.lineSeparator();
            exceptionMessage += mostRecentException.getMessage();
            e = new IOException(exceptionMessage, mostRecentException);
        } else {
            e = new IOException(exceptionMessage);
        }
        catchedExceptions.forEach(e::addSuppressed);
        throw e;
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setExecutor(Executor value);
        abstract Builder setHttpClient(OkHttpClient value);
        abstract Builder setValidResponseCodes(List<Integer> value);
        abstract Builder setMaxRetries(int value);

        abstract RequestExecutor autoBuild();

        public RequestExecutor build() {
            RequestExecutor requestExecutor = autoBuild();
            Preconditions.checkState(requestExecutor.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                            && requestExecutor.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                    , "Max retries out of range. Must be between "
                            + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

            return requestExecutor;
        }
    }
}
