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

import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.Request;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import okhttp3.HttpUrl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class RawReadRowsCursorsRequestProvider extends GenericRequestProvider{
    protected final static int DEFAULT_PARAMETER_NUMBER_OF_CURSORS = 100;

    public static Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_RawReadRowsCursorsRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public RawReadRowsCursorsRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("dbName")
                && parameters.getRequestParameters().get("dbName") instanceof String,
                "Request parameters must include dnName with a string value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("tableName")
                        && parameters.getRequestParameters().get("tableName") instanceof String,
                "Request parameters must include tableName");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();
        ImmutableList<String> rootParameters = ImmutableList.of("cursor", "numberOfCursors", "minLastUpdatedTime",
                "maxLastUpdatedTime");
        ImmutableList<String> filterParameters = ImmutableList.of("minLastUpdatedTime", "maxLastUpdatedTime");
        ImmutableList<Class> validClasses = ImmutableList.of(String.class, Integer.class, Long.class);

        // Check for number of cursors
        if (!requestParameters.getRequestParameters().containsKey("numberOfCursors")) {
            requestParameters = requestParameters.withRootParameter("numberOfCursors", DEFAULT_PARAMETER_NUMBER_OF_CURSORS);
        }

        // Add new cursor if specified
        if (cursor.isPresent()) {
            requestParameters = requestParameters.withRootParameter("cursor", cursor.get());
        }

        // Build path
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("dbName"));
        urlBuilder.addPathSegment("tables");
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("tableName"));
        urlBuilder.addPathSegment("cursors");

        // add the root parameters.
        requestParameters.getRequestParameters().entrySet().stream()
                .filter(entry -> rootParameters.contains(entry.getKey())
                        && validClasses.contains(entry.getValue().getClass()))
                .forEach(entry -> urlBuilder.addQueryParameter(entry.getKey(), String.valueOf(entry.getValue())));

        // add filter parameters.
        requestParameters.getFilterParameters().entrySet().stream()
                .filter(entry -> filterParameters.contains(entry.getKey())
                        && validClasses.contains(entry.getValue().getClass()))
                .forEach(entry -> urlBuilder.addQueryParameter(entry.getKey(), String.valueOf(entry.getValue())));

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder> {
        public abstract RawReadRowsCursorsRequestProvider build();
    }
}