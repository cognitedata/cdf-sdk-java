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

import com.cognite.client.dto.Asset;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class AssetParser {
    static final String logPrefix = "AssetParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an event json string to <code>Asset</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Asset parseAsset(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        Asset.Builder assetBuilder = Asset.newBuilder();

        // An asset must contain a name and id.
        if (root.path("id").isIntegralNumber()) {
            assetBuilder.setId(root.get("id").longValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: id. Item exerpt: "
                    + json
                    .substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
        }

        if (root.path("name").isTextual()) {
            assetBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: name");
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            assetBuilder.setExternalId(root.get("externalId").textValue());
        }
        if (root.path("rootId").isIntegralNumber()) {
            assetBuilder.setRootId(root.get("rootId").longValue());
        }
        if (root.path("parentId").isIntegralNumber()) {
            assetBuilder.setParentId(root.get("parentId").longValue());
        }
        if (root.path("parentExternalId").isTextual()) {
            assetBuilder.setParentExternalId(root.get("parentExternalId").textValue());
        }
        if (root.path("description").isTextual()) {
            assetBuilder.setDescription(root.get("description").textValue());
        }
        if (root.path("source").isTextual()) {
            assetBuilder.setSource(root.get("source").textValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            assetBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            assetBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            assetBuilder.setDataSetId(root.get("dataSetId").longValue());
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    assetBuilder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        if (root.path("labels").isArray()) {
            for (JsonNode node : root.path("labels")) {
                if (node.path("externalId").isTextual()) {
                    assetBuilder.addLabels(node.path("externalId").textValue());
                }
            }
        }

        if (root.path("geoLocation").isObject()) {
            assetBuilder.setGeoLocation(GeoParser.parseFeature(root.path("geoLocation").toString()));
        }

        // process the aggregates nested object
        if (root.path("aggregates").isObject()) {
            Asset.Aggregates.Builder aggregatesBuilder = Asset.Aggregates.newBuilder();
            JsonNode aggregates = root.get("aggregates");

            if (aggregates.path("childCount").isIntegralNumber()) {
                aggregatesBuilder.setChildCount(aggregates.get("childCount").intValue());
            }
            if (aggregates.path("depth").isIntegralNumber()) {
                aggregatesBuilder.setDepth(aggregates.get("depth").intValue());
            }
            if (aggregates.path("path").isArray()) {
                for (JsonNode node : aggregates.get("path")) {
                    if (node.path("id").isIntegralNumber()) {
                        aggregatesBuilder.addPath(node.get("id").longValue());
                    }
                }
            }
            assetBuilder.setAggregates(aggregatesBuilder.build());
        }

        return assetBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link Asset}.
     *
     * An insert item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(Asset element) {
        Preconditions.checkArgument(element.hasName(),
                "The asset must have a name in order to be created");
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("name", element.getName());

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        }

        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription());
        }
        if (element.hasParentExternalId()) {
            mapBuilder.put("parentExternalId", element.getParentExternalId());
        } else if (element.hasParentId()) {
            mapBuilder.put("parentId", element.getParentId());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.hasSource()) {
            mapBuilder.put("source", element.getSource());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            mapBuilder.put("labels", labels);
        }
        if (element.hasGeoLocation()) {
            mapBuilder.put("geoLocation", GeoParser.parseFeatureToMap(element.getGeoLocation()));
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from {@link Asset}.
     *
     * An update item object updates an existing asset object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(Asset element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.hasName()) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));
        }

        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        }
        if (element.hasParentExternalId()) {
            updateNodeBuilder.put("parentExternalId", ImmutableMap.of("set", element.getParentExternalId()));
        } else if (element.hasParentId()) {
            updateNodeBuilder.put("parentId", ImmutableMap.of("set", element.getParentId()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            updateNodeBuilder.put("labels", ImmutableMap.of("add", labels));
        }
        if (element.hasGeoLocation()) {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("set", GeoParser.parseFeatureToMap(element.getGeoLocation())));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request replace item object from <code>Asset</code>.
     *
     * A replace item object replaces an existing event object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(Asset element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update.");
        Preconditions.checkArgument(element.hasName(),
                "The asset must have a name in order to be written as an update replace.");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));

        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        } else {
            updateNodeBuilder.put("description", ImmutableMap.of("setNull", true));
        }

        if (element.hasParentExternalId()) {
            updateNodeBuilder.put("parentExternalId", ImmutableMap.of("set", element.getParentExternalId()));
        } else if (element.hasParentId()) {
            updateNodeBuilder.put("parentId", ImmutableMap.of("set", element.getParentId()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        } else {
            updateNodeBuilder.put("source", ImmutableMap.of("setNull", true));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        List<Map<String, String>> labels = new ArrayList<>();
        for (String label : element.getLabelsList()) {
            labels.add(ImmutableMap.of("externalId", label));
        }
        updateNodeBuilder.put("labels", ImmutableMap.of("set", labels));

        if (element.hasGeoLocation()) {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("set", GeoParser.parseFeatureToMap(element.getGeoLocation())));
        } else {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
