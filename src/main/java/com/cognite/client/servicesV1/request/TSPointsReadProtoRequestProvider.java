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
import com.cognite.client.servicesV1.util.TSIterationUtilities;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;

@AutoValue
public abstract class TSPointsReadProtoRequestProvider extends GenericRequestProvider {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int MAX_TS_ITEMS = 100;

    private final ObjectReader objectReader = mapper.reader();

    public static Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_TSPointsReadProtoRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public TSPointsReadProtoRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getItems().size() <= MAX_TS_ITEMS,
                "Datapoints can only be requested for maximum " + MAX_TS_ITEMS + " time series per request.");
        Preconditions.checkArgument(parameters.getItems().get(0).containsKey("id")
                || parameters.getItems().get(0).containsKey("externalId"),
                "The request must contain an id or externalId");
        for (Map<String, Object> item : parameters.getItems()) {
            checkItemPreconditions(item);
        }
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws Exception {
        final String randomString = RandomStringUtils.randomAlphanumeric(5);
        final String logPrefix = "Build read TS datapoints request - ";
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();

        // Check for limit
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            if (requestParameters.getRequestParameters().containsKey("aggregates")) {
                requestParameters = requestParameters.withRootParameter("limit",
                        ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS_AGG);
                LOG.info(logPrefix + "Request does not contain a *limit* parameter. Setting default limit: "
                        + ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS);
            } else {
                requestParameters = requestParameters.withRootParameter("limit",
                        ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS);
                LOG.info(logPrefix + "Request does not contain a *limit* parameter. Setting default limit: "
                        + ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS);
            }
        }

        if (cursor.isPresent()) {
            LOG.debug(logPrefix + "Adding cursor to the request.");
            LOG.debug(logPrefix + "Cursor: \r\n" + cursor.get());
            // The cursor should be a map of all (valid) TS items and their start timestamp
            ImmutableList<Map<String, Object>> cursorList = ImmutableList.copyOf(
                    objectReader.forType(new TypeReference<List<Map<String, Object>>>(){})
                            .<List<Map<String, Object>>>readValue(cursor.get()));
            ImmutableList<ImmutableMap<String, Object>> originalItems = requestParameters.getItems();
            List<Map<String, Object>> requestItems = new ArrayList<>();
            for (ImmutableMap<String, Object> item : originalItems) {
                Optional<Map<String, Object>> cursorItem = Optional.empty();
                // get the id of the item. Can be either externalId or id
                String externalId = (String) item.getOrDefault("externalId", "");
                if (!externalId.isEmpty()) {
                    cursorItem = getCursorObjectFromExternalId(cursorList, externalId);
                } else {
                    cursorItem = getCursorObjectFromId(cursorList, (Long) item.getOrDefault("id", 0l));
                }

                // Check that the id exits in the cursor map. If yes, add the item and set the new *start* parameter
                if (cursorItem.isPresent()) {
                    Map<String, Object> newItem = new HashMap<>();
                    newItem.putAll(item);
                    newItem.put("start", cursorItem.get().get("timestamp"));
                    requestItems.add(newItem);
                }
            }
            requestParameters = requestParameters.withItems(requestItems);
        }
        // Check that the request parameters contains items
        if (requestParameters.getItems().isEmpty()) {
            LOG.warn(logPrefix + "No items in the request parameters. Cannot build a valid request. \r\n {}",
                    requestParameters);
            throw new Exception(logPrefix + "No items in the request parameters. Cannot build a valid request.");
        }

        // Adjust the limit based on the number of TS in the request
        int newLimit = TSIterationUtilities.calculateLimit((Integer) requestParameters.getRequestParameters().get("limit"),
                requestParameters.getItems().size());

        requestParameters = requestParameters.withRootParameter("limit", newLimit);
        LOG.debug(logPrefix + "Adjusted the limit based on the number of TS items. New limit: " + newLimit);

        String outputJson = requestParameters.getRequestParametersAsJson();
        LOG.debug("TSPointsReadRequestProvider: Json request body: {}", outputJson);
        requestBuilder.header("Accept", "application/protobuf");
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

    private void checkItemPreconditions(Map<String, Object> item) {
        Preconditions.checkArgument(!item.containsKey("granularity")
                        && !item.containsKey("aggregates")
                        && !item.containsKey("start")
                        && !item.containsKey("end")
                        && !item.containsKey("limit")
                        && !item.containsKey("includeOutsidePoints") ,
                "Query specifications like aggregates, start, end, etc. must be specified at the root level.");
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder>{
        public abstract TSPointsReadProtoRequestProvider build();
    }
}
