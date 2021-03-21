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

package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing item object between Cognite api representations
 * (json and proto) and typed objects.
 */
public class ItemParser {
    static final Logger LOG = LoggerFactory.getLogger(ItemParser.class);
    static final String logPrefix = "ItemParser - ";
    static final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";
    static final ObjectMapper objectMapper = JsonUtil.getObjectMapperInstance();

    /**
     * Parses a <code>List</code> of json items (payload from the Cognite api) into an <code>List</code>
     * of <code>Item</code>.
     *
     * @param input
     * @return
     * @throws Exception
     */
    public static List<Item> parseItems(List<String> input) throws Exception {
        ImmutableList.Builder<Item> listBuilder = ImmutableList.builder();
        for (String item : input) {
            listBuilder.add(ItemParser.parseItem(item));
        }
        return listBuilder.build();
    }

    /**
     * Parses a <code>String</code> (json payload from the Cognite api) into an <code>Item</code>
     *
     * @return
     */
    public static Item parseItem(String itemJson) throws Exception {
        LOG.trace(logPrefix + "Start parsing item from Json to DTO.");
        LOG.trace(logPrefix + "Json input: \r\n {}", itemJson);
        try {
            JsonNode root = objectMapper.readTree(itemJson);
            Item.Builder itemBuilder = Item.newBuilder();

            // An item must contain an id or an externalId
            if (root.path("externalId").isTextual()) {
                itemBuilder.setExternalId(root.get("externalId").textValue());
            } else if (root.path("id").isIntegralNumber()) {
                itemBuilder.setId(root.get("id").longValue());
            } else if (root.path("legacyName").isTextual()) {
                itemBuilder.setLegacyName(root.get("legacyName").textValue());
            } else {
                String message = "Unable to parse attribute: id / externalId / legacyName. Item exerpt: " +
                        itemJson.substring(0, Math.min(itemJson.length(), MAX_LOG_ELEMENT_LENGTH));

                throw new Exception(message);
            }
            LOG.trace(logPrefix + "Item built: {}", itemBuilder.toString());
            return itemBuilder.build();
        } catch (Exception e) {
            LOG.error(logPrefix + parseErrorDefaultPrefix, e);
            throw e;
        }
    }

    /**
     * Builds a request item object from {@link Item}.
     *
     * An item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestItem(Item element) throws Exception {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder();

        if (element.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
            mapBuilder.put("externalId", element.getExternalId());
        } else if (element.getIdTypeCase() == Item.IdTypeCase.ID) {
            mapBuilder.put("id", element.getId());
        } else {
            String message = logPrefix + "Item does not contain id nor externalId: " + element.toString();
            LOG.error(message);
            throw new Exception(message);
        }

        return mapBuilder.build();
    }

    /**
     * Try parsing the specified Json path as a {@link Long}.
     *
     * If the Json node cannot be parsed, an empty {@link Optional} will be returned.
     * @param itemJson The Json string
     * @param fieldName The Json path to parse
     * @return The Json path as a {@link Long}, or an empty {@link Optional} if unable to parse it.
     */
    public static Optional<Long> parseLong(String itemJson, String fieldName) {
        Optional<Long> returnObject = Optional.empty();
        try {
            JsonNode root = objectMapper.readTree(itemJson);

            // Try parsing the field
            if (root.path(fieldName).isIntegralNumber()) {
                returnObject = Optional.of(root.get(fieldName).longValue());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: " + fieldName + ". Item exerpt: " +
                        itemJson.substring(0, Math.min(itemJson.length(), MAX_LOG_ELEMENT_LENGTH));

                LOG.debug(message);
            }
        } catch (Exception e) {
            LOG.warn(logPrefix + parseErrorDefaultPrefix, e);
        }

        return returnObject;
    }

    /**
     * Try parsing the specified Json path as a {@link String}.
     *
     * If the Json node cannot be parsed, an empty {@link Optional} will be returned.
     * @param itemJson The Json string
     * @param fieldName The Json path to parse
     * @return The Json path as a {@link String}, or an empty {@link Optional} if unable to parse it.
     */
    public static Optional<String> parseString(String itemJson, String fieldName) {
        Optional<String> returnObject = Optional.empty();
        try {
            JsonNode root = objectMapper.readTree(itemJson);

            // Try parsing the field
            if (root.path(fieldName).isTextual()) {
                returnObject = Optional.of(root.path(fieldName).textValue());
            } else if (root.path(fieldName).isValueNode()) {
                returnObject = Optional.of(root.path(fieldName).asText());
            } else if (root.path(fieldName).isObject()) {
                returnObject = Optional.of(root.path(fieldName).toString());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: " + fieldName + ". Item exerpt: " +
                        itemJson.substring(0, Math.min(itemJson.length(), MAX_LOG_ELEMENT_LENGTH));

                LOG.debug(message);
            }
        } catch (Exception e) {
            LOG.warn(logPrefix + parseErrorDefaultPrefix, e);
        }

        return returnObject;
    }
}
