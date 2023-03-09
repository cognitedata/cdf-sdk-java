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

/**
 * This class allows you to make a HTTP(S) request to an arbitrary Cognite Data Fusion API endpoint.
 *
 *
 */
@AutoValue
public abstract class CdfHttpRequest extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(CdfHttpRequest.class);

    protected final Map<String, String> headers = new HashMap<>();

    private static Builder builder() {
        return new AutoValue_CdfHttpRequest.Builder();
    }

    /**
     * Construct a new {@link CdfHttpRequest} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * The apiPath parameter should represent the path segments following {@code https://<cdfHost>/api/v1/projects/{project}/}.
     * For example, if you want to send a request to {@code https://<cdfHost>/api/v1/projects/{project}/}
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
                .withHeader("Accept", "application/json");
    }

    abstract CdfHttpRequest.Builder toBuilder();
    abstract URI getUri();
    abstract String getRequestBody();
    abstract RequestExecutor getRequestExecutor();

    public CdfHttpRequest withHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }

    public CdfHttpRequest withRequestBody(String requestBody)  {
        return toBuilder().setRequestBody(requestBody).build();
    }

    public CdfHttpRequest withRequestBody(Request request) throws Exception {
        return withRequestBody(request.getRequestParametersAsJson());
    }

    public CdfHttpRequest withRequestBody(Struct requestBody) throws InvalidProtocolBufferException {
        return withRequestBody(JsonFormat.printer().print(requestBody));
    }

    public ResponseBinary get() throws Exception {
        okhttp3.Request request = buildGenericRequest().get().build();
        return getRequestExecutor().executeRequest(request);
    }

    public ResponseBinary post() throws Exception {
        okhttp3.Request request = buildGenericRequest()
                .post(RequestBody.Companion.create(getRequestBody(), MediaType.get("application/json")))
                .build();

        return getRequestExecutor().executeRequest(request);
    }

    private okhttp3.Request.Builder buildGenericRequest() {
        okhttp3.Request.Builder reqBuilder = new okhttp3.Request.Builder()
                .headers(Headers.of(headers))
                .url(HttpUrl.parse(getUri().toString()));

        return reqBuilder;
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Builder setUri(URI value);
        abstract Builder setRequestBody(String value);
        abstract Builder setRequestExecutor(RequestExecutor value);

        abstract CdfHttpRequest build();
    }
}
