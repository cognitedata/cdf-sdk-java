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

package com.cognite.client;

import com.cognite.client.servicesV1.ResponseBinary;
import com.cognite.client.servicesV1.executor.RequestExecutor;
import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class allows you to make a HTTP(S) request to an arbitrary Cognite Data Fusion API endpoint.
 *
 * Authentication and retries will be automatically handled for you, but you need to supply the request URI,
 * the request body (if a post request) and optionally request headers.
 *
 * The response is returned in its raw form for you to interpret in your client code.
 */
@AutoValue
public abstract class CdfHttpRequest extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(CdfHttpRequest.class);

    private final Map<String, String> headers = new HashMap<>();

    private static Builder builder() {
        return new AutoValue_CdfHttpRequest.Builder();
    }

    /**
     * Construct a new {@link CdfHttpRequest} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * The requestUri parameter should follow the Cognite Data Fusion api URI pattern:
     * {@code https://<cdfHost>/api/v1/projects/{project}/{apiPathSegment}}. For example, if you want to send a request
     * to create {@code events}, the URI would be similar to {@code https://api.cognitedata.com/api/v1/projects/{project}/events}
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @param requestUri The (CDF) URI to send the request to.
     * @return The datasets api object.
     */
    public static CdfHttpRequest of(CogniteClient client, URI requestUri) {
        return CdfHttpRequest.builder()
                .setClient(client)
                .setUri(requestUri)
                .setRequestBody("")
                .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                        .withExecutor(client.getExecutorService())
                        .withMaxRetries(client.getClientConfig().getMaxRetries()))
                .build()
                .withHeader("Accept", "application/json")
                .withHeader("x-cdp-sdk", client.getClientConfig().getSdkIdentifier())
                .withHeader("x-cdp-app", client.getClientConfig().getAppIdentifier())
                .withHeader("x-cdp-clienttag", client.getClientConfig().getSessionIdentifier());
    }

    abstract CdfHttpRequest.Builder toBuilder();
    abstract URI getUri();
    abstract String getRequestBody();
    abstract RequestExecutor getRequestExecutor();

    /**
     * Add a header to the request. Repeat the calls to this method to add multiple headers.
     *
     * @param key the header key
     * @param value the header value.
     * @return the {@code CdfHttpRequest} object with the header set.
     */
    public CdfHttpRequest withHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }

    /**
     * Set the request body represented by a Json string.
     *
     * @param jsonRequestBody the request body in Json string representation
     * @return the {@code CdfHttpRequest} object with the request body set.
     */
    public CdfHttpRequest withRequestBody(String jsonRequestBody)  {
        return toBuilder().setRequestBody(jsonRequestBody).build();
    }

    /**
     * Sets the request body via a {@link Request} object. The {@link Request} object is a Java object representation of
     * a Json structure.
     *
     * Use {@code Request.withRequestParameters(Map<String, Object>)} to specify the entire request body using Java objects.
     * In this case you use Java objects to represent a Json request body. The mapping is fairly straight forward with
     * {@code Map<String, Object> -> Json object}, {@code List<T> -> Json array}, and {@code Java literals -> Json literals}.
     *
     * @param request the request object representing the Json request body.
     * @return the {@code CdfHttpRequest} object with the request body set.
     * @throws Exception
     */
    public CdfHttpRequest withRequestBody(Request request) throws Exception {
        return withRequestBody(request.getRequestParametersAsJson());
    }

    /**
     * Sets the request body via a {@link Struct} object. {@link Struct} is the Protobuf equivalent of a Json object.
     *
     * @param requestBody the {@link Struct} object representing the request body.
     * @return the {@code CdfHttpRequest} object with the request body set.
     * @throws InvalidProtocolBufferException
     */
    public CdfHttpRequest withRequestBody(Struct requestBody) throws InvalidProtocolBufferException {
        return withRequestBody(JsonFormat.printer().print(requestBody));
    }

    /**
     * Executes a {@code GET} request.
     *
     * @return The response of the request.
     * @throws Exception
     */
    public ResponseBinary get() throws Exception {
        okhttp3.Request request = buildGenericRequest().get().build();
        return getRequestExecutor().executeRequest(request);
    }

    /**
     * Executes a {@code POST} request.
     *
     * @return the response of the request.
     * @throws Exception
     */
    public ResponseBinary post() throws Exception {
        okhttp3.Request request = buildGenericRequest()
                .post(RequestBody.Companion.create(getRequestBody(), MediaType.get("application/json")))
                .build();

        return getRequestExecutor().executeRequest(request);
    }

    private okhttp3.Request.Builder buildGenericRequest() {
        return new okhttp3.Request.Builder()
                .headers(Headers.of(headers))
                .url(Objects.requireNonNull(HttpUrl.parse(getUri().toString())));
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Builder setUri(URI value);
        abstract Builder setRequestBody(String value);
        abstract Builder setRequestExecutor(RequestExecutor value);

        abstract CdfHttpRequest build();
    }
}
