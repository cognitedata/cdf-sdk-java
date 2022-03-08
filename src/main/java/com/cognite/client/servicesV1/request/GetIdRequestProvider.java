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

package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;

import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Builds request to get results from an async api job based on an id.
 *
 * Used by various context api services as most context services are based on an async api pattern.
 *
 * Job id is specified via the {@link Request}.
 */
@AutoValue
public abstract class GetIdRequestProvider extends GenericRequestProvider{

    static Builder builder() {
        return new AutoValue_GetIdRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    /**
     * Returns a request provider that will get job results from a jobId endpoint.
     *
     * @return
     */
    public static GetIdRequestProvider of(String endpoint) {
        return GetIdRequestProvider.builder()
                .setEndpoint(endpoint)
                .build();
    }

    public abstract Builder toBuilder();

    public GetIdRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("id")
                && (parameters.getRequestParameters().get("id") instanceof Integer
                        || parameters.getRequestParameters().get("id") instanceof Long),
                "Request parameters must include id with an int/long value");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder
                .addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("id")));
        requestBuilder.url(urlBuilder.build());

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder> {
        public abstract GetIdRequestProvider build();
    }
}