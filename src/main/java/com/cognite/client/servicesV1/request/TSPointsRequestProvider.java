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
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

@AutoValue
public abstract class TSPointsRequestProvider extends GenericRequestProvider {
    private final ObjectReader objectReader = JsonUtil.getObjectMapperInstance().reader();

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
        final String logPrefix = "Build read TS datapoints request - ";
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();

        // Check for limit
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            requestParameters = requestParameters.withRootParameter("limit",
                    ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS);
        }

        if (cursor.isPresent()) {
            LOG.debug(logPrefix + "Adding cursor to the request.");
            LOG.debug(logPrefix + "Cursor: \r\n" + cursor.get());
            // The cursor should be a map of all (valid) TS items and their start timestamp
            List<Map<String, Object>> cursorList = objectReader.forType(new TypeReference<List<Map<String, Object>>>(){})
                            .<List<Map<String, Object>>>readValue(cursor.get());

            ImmutableList<ImmutableMap<String, Object>> originalItems = requestParameters.getItems();
            List<Map<String, Object>> requestItems = new ArrayList<>();
            for (ImmutableMap<String, Object> item : originalItems) {
                Optional<Map<String, Object>> cursorItem;
                // get the id of the item. Can be either externalId or id
                String externalId = (String) item.getOrDefault("externalId", "");
                if (!externalId.isEmpty()) {
                    cursorItem = getCursorObjectFromExternalId(cursorList, externalId);
                } else {
                    cursorItem = getCursorObjectFromId(cursorList, (Long) item.getOrDefault("id", 0l));
                }

                // Check that the id exits in the cursor map. If yes, add the item
                if (cursorItem.isPresent()) {
                    Map<String, Object> newItem = new HashMap<>();
                    newItem.putAll(item);
                    newItem.put("cursor", cursorItem.get().get("nextCursor"));
                    requestItems.add(newItem);

                    // remove the cursor from the original list so we can do some "accounting" at the end.
                    cursorList.remove(cursorItem);
                }
            }

            if (cursorList.size() > 0) {
                // We have some cursors that have not been applied--this should not happen
                LOG.warn(logPrefix + "Mismatch between nextCursor and the data points request. {} cursors don't have a "
                        + "corresponding request item. Surplus cursors: \n{}",
                        cursorList.size(),
                        cursorList);
            }

            requestParameters = requestParameters.withItems(requestItems);
        }

        String outputJson = requestParameters.getRequestParametersAsJson();
        LOG.debug("Json request body: {}", outputJson);
        return requestBuilder.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    private Optional<Map<String, Object>> getCursorObjectFromId(List<Map<String, Object>> cursorList, long id) {
        ImmutableMap<String, Object> cursorObject = null;
        for (Map<String, Object> item : cursorList) {
            if ((Long) item.getOrDefault("id", 0l) == id) {
                cursorObject = ImmutableMap.copyOf(item);
            }
        }
        return Optional.ofNullable(cursorObject);
    }

    private Optional<Map<String, Object>> getCursorObjectFromExternalId(List<Map<String, Object>> cursorList, String externalId) {
        ImmutableMap<String, Object> cursorObject = null;
        for (Map<String, Object> item : cursorList) {
            if (((String) item.getOrDefault("externalId", "")).equals(externalId)) {
                cursorObject = ImmutableMap.copyOf(item);
            }
        }
        return Optional.ofNullable(cursorObject);
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder>{
        public abstract TSPointsRequestProvider build();
    }
}
