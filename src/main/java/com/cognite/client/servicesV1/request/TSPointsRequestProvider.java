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
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class TSPointsRequestProvider extends GenericRequestProvider {

    public static Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_TSPointsRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public TSPointsRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getItems().size() == 1,
                "Datapoints can only be requested for a single time series per request.");
        Preconditions.checkArgument(parameters.getItems().get(0).containsKey("id")
                || parameters.getItems().get(0).containsKey("externalId"),
                "The request must contain an id or externalId");
        Preconditions.checkArgument(!parameters.getItems().get(0).containsKey("granularity")
                && !parameters.getItems().get(0).containsKey("aggregates")
                && !parameters.getItems().get(0).containsKey("start")
                && !parameters.getItems().get(0).containsKey("end")
                && !parameters.getItems().get(0).containsKey("limit")
                && !parameters.getItems().get(0).containsKey("includeOutsidePoints") ,
                "Query specifications like aggregates, start, end, etc. must be specified at the root level.");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();

        // Check for limit
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            requestParameters = requestParameters.withRootParameter("limit",
                    ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS);
        }

        if (cursor.isPresent()) {
            requestParameters = requestParameters.withRootParameter("start", cursor.get());
        }

        String outputJson = requestParameters.getRequestParametersAsJson();
        LOG.debug("Json request body: {}", outputJson);
        return requestBuilder.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder>{
        public abstract TSPointsRequestProvider build();
    }
}
