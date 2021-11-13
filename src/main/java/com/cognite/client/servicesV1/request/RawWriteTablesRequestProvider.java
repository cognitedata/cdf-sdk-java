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
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Request provider purpose-built for creating/writing tables in raw. It is a slight variation of the
 * generic PostJsonRequestProvider where we look for the key "ensureParent" and add that to the
 * request query parameters instead of the json payload.
 */
@AutoValue
public abstract class RawWriteTablesRequestProvider extends GenericRequestProvider {

    public static Builder builder() {
        return new AutoValue_RawWriteTablesRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setEndpoint(ConnectorConstants.DEFAULT_ENDPOINT)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public RawWriteTablesRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Add "ensureParent" to the query part of the URL
        if (getRequest().getRequestParameters().containsKey("ensureParent")
                && getRequest().getRequestParameters().get("ensureParent") instanceof Boolean) {
            urlBuilder.addQueryParameter("ensureParent",
                    String.valueOf(getRequest().getRequestParameters().get("ensureParent")));
        }
        requestBuilder.url(urlBuilder.build());

        // Build a "clean" request parameter object containing only the items from the input.
        Request requestParameters = Request.create()
                .withItems(getRequest().getItems());

        String outputJson = requestParameters.getRequestParametersAsJson();
        return requestBuilder.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder>{
        public abstract RawWriteTablesRequestProvider build();
    }
}
