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

import com.cognite.client.config.AuthConfig;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.*;

/**
 * This class represents the Cognite Data Fusion API request parameters.
 *
 * The available parameters depend on which API endpoint you are working towards. The parameters mirrors
 * what is available in the api. For example, which filters you can use. Please refer to the Cognite API documentation
 * {@code https://docs.cognite.com/api/v1/} for reference.
 *
 * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
 */
@AutoValue
public abstract class Request implements Serializable {
    private final ObjectReader objectReader = JsonUtil.getObjectMapperInstance().reader();
    private final ObjectWriter objectWriter = JsonUtil.getObjectMapperInstance().writer();

    private static Builder builder() {
        return new AutoValue_Request.Builder();
    }

    public static Request create() {
        return Request.builder().build();
    }

    /**
     * Returns the object representation of the composite request body. It is similar to the Cognite API Json
     * request body, with {@code Map<String, Object>} as the Json container, {@code List} as the
     * Json array.
     *
     * @return
     */
    public abstract ImmutableMap<String, Object> getRequestParameters();

    /**
     * For internal use only.
     *
     * Returns the protobuf request body.
     * @return
     */
    @Nullable
    public abstract Message getProtoRequestBody();

    /**
     * Returns the project configuration for a request. The configuration includes host (optional),
     * project/tenant and key.
     *
     * @return
     */
    @Nullable
    public abstract AuthConfig getAuthConfig();

    abstract Builder toBuilder();

    /**
     * Adds the complete request parameter structure based on Java objects. Calling this method will overwrite
     * any previously added parameters.
     *
     * - All keys must be String.
     * - Values can be primitives or containers
     * - Valid primitives are String, Integer, Double, Float, Long, Boolean.
     * - Valid containers are Map (Json Object) and List (Json array).
     *
     * @param requestParameters
     * @return The request object with the parameter applied.
     */
    public Request withRequestParameters(Map<String, Object> requestParameters) {
        checkArgument(requestParameters != null, "Input cannot be null or empty.");
        return toBuilder().setRequestParameters(requestParameters).build();
    }

    /**
     * Adds the complete request body as a protobuf object.
     *
     * @param requestBody
     * @return The request object with the parameter applied.
     */
    public Request withProtoRequestBody(Message requestBody) {
        return toBuilder().setProtoRequestBody(requestBody).build();
    }

    /**
     * Adds the complete request parameter structure based on Json. Calling this method will overwrite
     * any previously added parameters.
     *
     * @param value
     * @return The request object with the parameter applied.
     */
    public Request withRequestJson(String value) throws Exception {
        checkArgument(value != null && !value.isEmpty(), "Request Json cannot be null or empty.");

        ImmutableMap<String, Object> fromJson = ImmutableMap.copyOf(
                objectReader.forType(new TypeReference<Map<String,Object>>(){}).<Map<String, Object>>readValue(value));
        return toBuilder().setRequestParameters(fromJson).build();
    }

    /**
     * Adds a new parameter to the root level.
     *
     * @param key
     * @param value
     * @return The request object with the parameter applied.
     */
    public Request withRootParameter(String key, Object value) {
        checkArgument(key != null && !key.isEmpty(), "Key cannot be null or empty.");
        checkArgument(value != null, "Value cannot be null.");

        HashMap<String, Object> tempMap = new HashMap<>();
        tempMap.putAll(getRequestParameters());
        tempMap.put(key, value);

        return toBuilder().setRequestParameters(tempMap).build();
    }

    /**
     * Adds a new parameter to the filter node.
     *
     * @param key
     * @param value
     * @return The request object with the parameter applied.
     */
    public Request withFilterParameter(String key, Object value) {
        checkArgument(key != null && !key.isEmpty(), "Key cannot be null or empty.");
        checkArgument(value != null, "Value cannot be null.");

        Map<String, Object> tempMapRoot = new HashMap<>(getRequestParameters());

        // Check the existence of the filter node.
        if (!tempMapRoot.containsKey("filter") || !(tempMapRoot.get("filter") instanceof Map)) {
            tempMapRoot.put("filter", ImmutableMap.<String, Object>of());
        }

        Map<String, Object> tempMapFilter = new HashMap<>((Map<String, Object>)tempMapRoot.get("filter"));
        tempMapFilter.put(key, value);

        tempMapRoot.put("filter", ImmutableMap.copyOf(tempMapFilter));
        return toBuilder().setRequestParameters(tempMapRoot).build();
    }

    /**
     * Adds a new parameter to the filter.metadata node.
     *
     * @param key
     * @param value
     * @return The request object with the parameter applied.
     */
    public Request withFilterMetadataParameter(String key, String value) {
        checkArgument(key != null && !key.isEmpty(), "Key cannot be null or empty.");
        checkArgument(value != null, "Value cannot be null.");

        HashMap<String, Object> tempMapRoot = new HashMap<>(getRequestParameters());

        // Check the existence of the filter node.
        if (!tempMapRoot.containsKey("filter") || !(tempMapRoot.get("filter") instanceof Map)) {
            tempMapRoot.put("filter", ImmutableMap.<String, Object>of());
        }
        HashMap<String, Object> tempMapFilter = new HashMap<>((Map)tempMapRoot.get("filter"));

        // Check the existence of the the filter.metadata node.
        if (!tempMapFilter.containsKey("metadata") || !(tempMapFilter.get("metadata") instanceof Map)) {
            tempMapFilter.put("metadata", ImmutableMap.<String, Object>of());
        }

        HashMap<String, String> tempMapFilterMetadata = new HashMap<>((Map) tempMapFilter.get("metadata"));
        tempMapFilterMetadata.put(key, value);

        tempMapFilter.put("metadata", ImmutableMap.copyOf(tempMapFilterMetadata));
        tempMapRoot.put("filter", ImmutableMap.copyOf(tempMapFilter));
        return toBuilder().setRequestParameters(tempMapRoot).build();
    }

    /**
     * Sets the items array to the specified input list. The list represents an array of objects via
     * Java Map. That is, an instance of Map equals a Json object.
     *
     * For most write operations the maximum number of items per request is 1k. For time series datapoints
     * the maximum number of items is 10k, with a maximum of 100k data points.
     *
     * @param items
     * @return The request object with the parameter applied.
     */
    public Request withItems(List<? extends Map<String, Object>> items) {
        checkNotNull(items, "Items cannot be null.");
        checkArgument(items.size() <= 10000, "Number of items cannot exceed 10k.");

        HashMap<String, Object> tempMapRoot = new HashMap<>();
        tempMapRoot.putAll(getRequestParameters());
        tempMapRoot.put("items", items);
        return toBuilder().setRequestParameters(tempMapRoot).build();
    }

    /**
     * Convenience method for setting the external id for requesting an item.
     *
     * You can use this method when requesting a data item by id, for example when requesting the data points
     * from a time series.
     *
     * @param externalId
     * @return The request object with the parameter applied.
     */
    public Request withItemExternalId(String externalId) {
        checkNotNull(externalId, "ExternalId cannot be null.");

        List<Map<String, Object>> items = ImmutableList.of(
                ImmutableMap.of("externalId", externalId)
        );

        return withItems(items);
    }

    /**
     * Convenience method for setting the external id for requesting an item.
     *
     * You can use this method when requesting a data item by id, for example when requesting the data points
     * from a time series.
     *
     * @param internalId
     * @return The request object with the parameter applied.
     */
    public Request withItemInternalId(long internalId) {
        List<Map<String, Object>> items = ImmutableList.of(
                ImmutableMap.of("id", internalId)
        );

        return withItems(items);
    }

    /**
     * Convenience method for adding the database name when reading from Cognite.Raw.
     *
     * @param dbName The name of the database to read from.
     * @return The request object with the parameter applied.
     */
    public Request withDbName(String dbName) {
        checkArgument(dbName != null && !dbName.isEmpty(), "Database name cannot be null or empty.");

        return this.withRootParameter("dbName", dbName);
    }

    /**
     * Convenience method for adding the table name when reading from Cognite.Raw.
     *
     * @param tableName The name of the database to read from.
     * @return The request object with the parameter applied.
     */
    public Request withTableName(String tableName) {
        checkArgument(tableName != null && !tableName.isEmpty(), "Database name cannot be null or empty.");

        return this.withRootParameter("tableName", tableName);
    }

    /**
     * Sets the project configuration for a request. The configuration includes host (optional),
     * project/tenant and key.
     *
     * You can use this method if you need detailed, per-request control over auth config. In most cases, however,
     * you will configure this once on the {@link CogniteClient}.
     *
     * @param config The auth config
     * @return The request object with the configuration applied.
     */
    public Request withAuthConfig(AuthConfig config) {
        return toBuilder().setAuthConfig(config).build();
    }

    public String getRequestParametersAsJson() throws JsonProcessingException {
        return objectWriter.writeValueAsString(getRequestParameters());
    }

    /**
     * Returns the collection of filter parameters as a Map. The filter parameter must have a key of type
     * String in order to be included in the return collection.
     *
     * Please note that all entries (with a key of type {@code String}) under the filter node are returned, potentially
     * including nested collections (such as metadata).
     *
     * @return
     */
    public ImmutableMap<String, Object> getFilterParameters() {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // filter parameters exists and must be a map.
        if (getRequestParameters().containsKey("filter") && getRequestParameters().get("filter") instanceof Map) {
            ((Set<Map.Entry>) ((Map) getRequestParameters().get("filter")).entrySet()).stream()
                    .filter(entry -> entry.getKey() instanceof String)
                    .forEach(entry -> mapBuilder.put((String) entry.getKey(), entry.getValue()));

            return mapBuilder.build();
        } else {
            return ImmutableMap.<String, Object>of();
        }
    }

    /**
     * Returns the collection of metadata specific filter parameters as a Map. The parameters must have both a key and
     * value of type String in order to be included in the return collection.
     *
     * @return
     */
    public ImmutableMap<String, String> getMetadataFilterParameters() {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        // metadata filter parameters exists and must be a map.
        if (getFilterParameters().containsKey("metadata") && getFilterParameters().get("metadata") instanceof Map) {
            ((Set<Map.Entry>) ((Map) getFilterParameters().get("metadata")).entrySet()).stream()
                    .filter(entry -> entry.getKey() instanceof String && entry.getValue() instanceof String)
                    .forEach(entry -> mapBuilder.put((String) entry.getKey(), (String) entry.getValue()));

            return mapBuilder.build();
        } else {
            return ImmutableMap.<String, String>of();
        }
    }

    /**
     * Returns the list of items. This is typically the main payload of a write request (create, update or delete).
     *
     * @return
     */
    public ImmutableList<ImmutableMap<String, Object>> getItems() {
        if (getRequestParameters().get("items") == null) return ImmutableList.<ImmutableMap<String, Object>>of();

        checkState(getRequestParameters().get("items") instanceof List, "Items are not of the type List");

        List<ImmutableMap<String, Object>> tempList = new ArrayList<>();
        for (Map<String, Object> item : ((List<Map<String, Object>>) getRequestParameters().get("items"))) {
            tempList.add(ImmutableMap.copyOf(item));
        }

        return ImmutableList.copyOf(tempList);
    }



    @AutoValue.Builder
    public abstract static class Builder {

        abstract ImmutableMap.Builder<String, Object> requestParametersBuilder();
        abstract Builder setRequestParameters(Map<String, Object> value);
        abstract Builder setProtoRequestBody(Message value);
        abstract Builder setAuthConfig(AuthConfig value);

        public abstract Request build();

        public Builder addParameter(String key, Object value) {
            checkArgument(key != null && !key.isEmpty(), "Key cannot be null or empty");
            checkArgument(value != null, "Value cannot be null");
            requestParametersBuilder().put(key, value);
            return this;
        }
    }
}
