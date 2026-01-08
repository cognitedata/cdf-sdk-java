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

import com.cognite.client.dto.LimitValue;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.ResponseBinary;
import com.cognite.client.servicesV1.parser.LimitValueParser;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class represents the Cognite Limit Values API endpoint.
 *
 * It provides methods for reading {@link LimitValue} objects.
 *
 * Note: This API is currently in alpha and requires the cdf-version header.
 */
@AutoValue
public abstract class LimitValues extends ApiBase {

    private static final String CDF_VERSION_HEADER = "cdf-version";
    private static final String CDF_VERSION_VALUE = "20230101-alpha";
    private static final ObjectMapper objectMapper = JsonUtil.getObjectMapperInstance();

    private static Builder builder() {
        return new AutoValue_LimitValues.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(LimitValues.class);

    /**
     * Constructs a new {@link LimitValues} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the limit values api object.
     */
    public static LimitValues of(CogniteClient client) {
        return LimitValues.builder()
                .setClient(client)
                .build();
    }

    /**
     * Retrieves a specific limit value by its ID.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     LimitValue limitValue = client.limitValues().retrieve("my-limit-id");
     * }
     * </pre>
     *
     * @param limitId The ID of the limit value to retrieve.
     * @return The retrieved {@link LimitValue}.
     * @throws Exception if the retrieval fails.
     */
    public LimitValue retrieve(String limitId) throws Exception {
        Preconditions.checkArgument(limitId != null && !limitId.isBlank(),
                "limitId cannot be null or blank.");

        String loggingPrefix = "retrieve() - ";
        LOG.debug(loggingPrefix + "Retrieving limit value with id: {}", limitId);

        URI requestUri = buildUri("limits/values/" + limitId);

        ResponseBinary response = getClient().experimental().cdfHttpRequest(requestUri)
                .withHeader(CDF_VERSION_HEADER, CDF_VERSION_VALUE)
                .get();

        if (!response.getResponse().isSuccessful()) {
            throw new Exception("Failed to retrieve limit value. Response code: "
                    + response.getResponse().code()
                    + ", message: " + response.getResponse().message());
        }

        String responseBody = response.getResponseBodyBytes().toStringUtf8();
        return parseLimitValue(responseBody);
    }

    /**
     * Returns all {@link LimitValue} objects using pagination.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<LimitValue> allLimitValues = new ArrayList<>();
     *     client.limitValues()
     *             .list()
     *             .forEachRemaining(allLimitValues::addAll);
     * }
     * </pre>
     *
     * @return An {@link Iterator} to page through the results.
     */
    public Iterator<List<LimitValue>> list() throws Exception {
        return list(Request.create());
    }

    /**
     * Returns {@link LimitValue} objects that match the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite API.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<LimitValue> limitValues = new ArrayList<>();
     *     client.limitValues()
     *             .list(Request.create()
     *                     .withFilterParameter("prefix", Map.of(
     *                         "property", List.of("limitId"),
     *                         "value", "atlas.")))
     *             .forEachRemaining(limitValues::addAll);
     * }
     * </pre>
     *
     * @param requestParameters The filters to use for retrieving limit values.
     * @return An {@link Iterator} to page through the results.
     */
    public Iterator<List<LimitValue>> list(Request requestParameters) {
        return new LimitValuesIterator(requestParameters);
    }

    /**
     * Builds the URI for the limit values API endpoint.
     *
     * @param pathSegment The path segment to append (e.g., "limits/values/list")
     * @return The complete URI
     */
    private URI buildUri(String pathSegment) {
        String baseUrl = getClient().getBaseUrl();
        String project = getClient().getProject();
        return URI.create(String.format("%s/api/v1/projects/%s/%s",
                baseUrl, project, pathSegment));
    }

    /**
     * Parses a JSON string to a LimitValue object.
     */
    private LimitValue parseLimitValue(String json) {
        try {
            return LimitValueParser.parseLimitValue(json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse LimitValue from JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Iterator implementation for paginated limit values listing.
     */
    private class LimitValuesIterator implements Iterator<List<LimitValue>> {
        private final Request requestParameters;
        private String cursor = null;
        private boolean hasMore = true;

        public LimitValuesIterator(Request requestParameters) {
            this.requestParameters = requestParameters;
        }

        @Override
        public boolean hasNext() {
            return hasMore;
        }

        @Override
        public List<LimitValue> next() {
            try {
                return fetchNextBatch();
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch next batch of limit values", e);
            }
        }

        private List<LimitValue> fetchNextBatch() throws Exception {
            URI requestUri = buildUri("limits/values/list");

            // Build request body with cursor if available
            Map<String, Object> requestBody = new java.util.HashMap<>(requestParameters.getRequestParameters());

            if (!requestBody.containsKey("limit")) {
                requestBody.put("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE);
            }

            if (cursor != null) {
                requestBody.put("cursor", cursor);
            }

            Request request = Request.create().withRequestParameters(requestBody);

            ResponseBinary response = getClient().experimental().cdfHttpRequest(requestUri)
                    .withRequestBody(request)
                    .withHeader(CDF_VERSION_HEADER, CDF_VERSION_VALUE)
                    .post();

            if (!response.getResponse().isSuccessful()) {
                throw new Exception("Failed to list limit values. Response code: "
                        + response.getResponse().code()
                        + ", message: " + response.getResponse().message());
            }

            String responseBody = response.getResponseBodyBytes().toStringUtf8();
            JsonNode root = objectMapper.readTree(responseBody);

            // Parse items
            List<LimitValue> results = new ArrayList<>();
            JsonNode itemsNode = root.path("items");
            if (itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    results.add(LimitValueParser.parseLimitValue(itemNode));
                }
            }

            // Check for next cursor
            JsonNode nextCursorNode = root.path("nextCursor");
            if (nextCursorNode.isTextual() && !nextCursorNode.textValue().isEmpty()) {
                cursor = nextCursorNode.textValue();
                hasMore = true;
            } else {
                hasMore = false;
            }

            return results;
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract LimitValues build();
    }
}

