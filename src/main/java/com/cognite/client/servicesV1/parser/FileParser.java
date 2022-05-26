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

import com.cognite.client.dto.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class FileParser {
    static final String logPrefix = "FileParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a file metadata/header json string to <code>FileMetadata</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static FileMetadata parseFileMetadata(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        FileMetadata.Builder fileMetaBuilder = FileMetadata.newBuilder();

        // A file must contain id, name, isUploaded, created and updatedTime.
        if (root.path("id").isIntegralNumber()) {
            fileMetaBuilder.setId(root.get("id").longValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("id", json));
        }

        if (root.path("name").isTextual()) {
            fileMetaBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("name", json));
        }

        if (root.path("directory").isTextual()) {
            fileMetaBuilder.setDirectory(root.get("directory").textValue());
        }

        if (root.path("uploaded").isBoolean()) {
            fileMetaBuilder.setUploaded(root.get("uploaded").booleanValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("uploaded", json));
        }

        if (root.path("createdTime").isIntegralNumber()) {
            fileMetaBuilder.setCreatedTime(root.get("createdTime").longValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("createdTime", json));
        }

        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            fileMetaBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("lastUpdatedTime", json));
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            fileMetaBuilder.setExternalId(root.get("externalId").textValue());
        }
        if (root.path("uploadedTime").isIntegralNumber()) {
            fileMetaBuilder.setUploadedTime(root.get("uploadedTime").longValue());
        }
        if (root.path("source").isTextual()) {
            fileMetaBuilder.setSource(root.get("source").textValue());
        }
        if (root.path("mimeType").isTextual()) {
            fileMetaBuilder.setMimeType(root.get("mimeType").textValue());
        }
        if (root.path("sourceCreatedTime").isIntegralNumber()) {
            fileMetaBuilder.setSourceCreatedTime(root.get("sourceCreatedTime").longValue());
        }
        if (root.path("sourceModifiedTime").isIntegralNumber()) {
            fileMetaBuilder.setSourceModifiedTime(root.get("sourceModifiedTime").longValue());
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            fileMetaBuilder.setDataSetId(root.get("dataSetId").longValue());
        }

        if (root.path("securityCategories").isArray()) {
            for (JsonNode node : root.path("securityCategories")) {
                if (node.isIntegralNumber()) {
                    fileMetaBuilder.addSecurityCategories(node.longValue());
                }
            }
        }

        if (root.path("assetIds").isArray()) {
            for (JsonNode node : root.path("assetIds")) {
                if (node.isIntegralNumber()) {
                    fileMetaBuilder.addAssetIds(node.longValue());
                }
            }
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root
                    .path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    fileMetaBuilder
                            .putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        if (root.path("labels").isArray()) {
            for (JsonNode node : root.path("labels")) {
                if (node.path("externalId").isTextual()) {
                    fileMetaBuilder.addLabels(node.path("externalId").textValue());
                }
            }
        }

        if (root.path("geoLocation").isObject()) {
            final JsonNode locationNode = root.path("geoLocation");
            final GeoLocation.Builder geoLocationBuilder = GeoLocation.newBuilder();
            geoLocationBuilder.setType(locationNode.path("type").textValue());

            if (locationNode.path("properties").isObject()) {
                Struct.Builder structBuilder = Struct.newBuilder();
                JsonFormat.parser()
                        .merge(objectMapper.writeValueAsString(locationNode.get("properties")), structBuilder);
                geoLocationBuilder.setProperties(structBuilder.build());
            }
            if (locationNode.path("geometry").isObject()) {
                Preconditions.checkNotNull(locationNode.path("geometry").path("type"));
                Preconditions.checkArgument(locationNode.path("geometry").path("type").isTextual());

                final HashMap<String, Function<JsonNode, GeoLocationGeometry>> functionHashMap = new HashMap<>();
                functionHashMap.put("Point", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesPoint(getPointCoordinates(node.path("coordinates")))
                        .build()
                );

                functionHashMap.put("MultiPoint", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesMultiPoint(getMultiPointCoordinates(node.path("coordinates")))
                        .build()
                );

                functionHashMap.put("Polygon", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesPolygon(getPolygonCoordinates(node.path("coordinates")))
                        .build()
                );

                functionHashMap.put("MultiPolygon", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesMultiPolygon(getMultiPolygonCoordinates(node.path("coordinates")))
                        .build()
                );

                functionHashMap.put("LineString", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesLineString(getLineCoordinates(node.path("coordinates")))
                        .build()
                );

                functionHashMap.put("MultiLineString", node -> GeoLocationGeometry.newBuilder()
                        .setCoordinatesMultiLine(getMultiLineCoordinates(node.path("coordinates")))
                        .build()
                );

                geoLocationBuilder.setGeometry(
                        functionHashMap
                                .getOrDefault(locationNode.path("geometry").path("type").textValue(),
                                        node -> {
                                            // we should not be here, but who knows...
                                            throw new IllegalStateException("JSON has unknown type: " + node.path("type").textValue());
                                        })
                                .apply(locationNode.path("geometry")));
            } else {
                throw new IllegalStateException("FileMetadata: geometry property is required inside geoLocation");
            }
            fileMetaBuilder.setGeoLocation(geoLocationBuilder.build());
        }

        return fileMetaBuilder.build();
    }

    private static PointCoordinates getPointCoordinates(JsonNode node) {
        final PointCoordinates.Builder coordinatesBuilder = PointCoordinates.newBuilder();
        for (JsonNode n : node) {
            if (n.isIntegralNumber()) {
                coordinatesBuilder.addCoordinates(n.doubleValue());
            }
        }
        return coordinatesBuilder.build();
    }

    private static MultiPointCoordinates getMultiPointCoordinates(JsonNode node) {
        final MultiPointCoordinates.Builder coordinatesBuilder = MultiPointCoordinates.newBuilder();
        for (JsonNode n : node) {
            coordinatesBuilder.addCoordinates(getPointCoordinates(n));
        }
        return coordinatesBuilder.build();
    }

    private static PolygonCoordinates getPolygonCoordinates(JsonNode node) {
        final PolygonCoordinates.Builder coordinatesBuilder = PolygonCoordinates.newBuilder();
        for (JsonNode n : node) {
            coordinatesBuilder.addCoordinates(getMultiPointCoordinates(n));
        }
        return coordinatesBuilder.build();
    }

    private static MultiPolygonCoordinates getMultiPolygonCoordinates(JsonNode node) {
        final MultiPolygonCoordinates.Builder coordinatesBuilder = MultiPolygonCoordinates.newBuilder();
        for (JsonNode n : node) {
            coordinatesBuilder.addCoordinates(getPolygonCoordinates(n));
        }
        return coordinatesBuilder.build();
    }

    private static LineCoordinates getLineCoordinates(JsonNode node) {
        final LineCoordinates.Builder coordinatesBuilder = LineCoordinates.newBuilder();
        for (JsonNode n : node) {
            coordinatesBuilder.addCoordinates(getPointCoordinates(n));
        }
        return coordinatesBuilder.build();
    }

    private static MultiLineCoordinates getMultiLineCoordinates(JsonNode node) {
        final MultiLineCoordinates.Builder coordinatesBuilder = MultiLineCoordinates.newBuilder();
        for (JsonNode n : node) {
            coordinatesBuilder.addCoordinates(getLineCoordinates(n));
        }
        return coordinatesBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>FileMetadata</code>.
     *
     * An insert item object creates a new file (metadata) data object in the Cognite system.
     *
     * @param element
     * @return
     * @throws Exception
     */
    public static Map<String, Object> toRequestInsertItem(FileMetadata element) throws Exception {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (element.hasName()) {
            mapBuilder.put("name", element.getName());
        } else {
            throw new Exception("Unable to find attribute [name] in the file metadata/header object. "
                    + "externalId: [" + element.getExternalId() + "].");
        }

        // Optional fields
        if (element.hasDirectory()) {
            mapBuilder.put("directory", element.getDirectory());
        }

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        }

        if (element.hasSource()) {
            mapBuilder.put("source", element.getSource());
        }
        if (element.hasMimeType()) {
            mapBuilder.put("mimeType", element.getMimeType());
        }
        if (element.hasSourceCreatedTime()) {
            mapBuilder.put("sourceCreatedTime", element.getSourceCreatedTime());
        }
        if (element.hasSourceModifiedTime()) {
            mapBuilder.put("sourceModifiedTime", element.getSourceModifiedTime());
        }
        if (element.getAssetIdsCount() > 0) {
            mapBuilder.put("assetIds", element.getAssetIdsList());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
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
            mapBuilder.put("geoLocation", getGeoLocationJson(element.getGeoLocation()));
        }

        return mapBuilder.build();
    }

    static Map<String, Object> getGeoLocationJson(GeoLocation location) {
        final HashMap<String, Object> geoLocation = new HashMap<>();
        geoLocation.put("type", location.getType());
        geoLocation.put("geometry", getGeometry(location.getGeometry()));
        if (location.hasProperties()) {
            geoLocation.put("properties", RawParser.parseStructToMap(location.getProperties()));
        }
        return geoLocation;
    }

    private static Map<String, Object> getGeometry(GeoLocationGeometry geometry) {
        final HashMap<String, Object> root = new HashMap<>();
        // List referencing order of protobuf definition
        final List<String> names = List.of(
                "Point", "MultiPoint", "Polygon", "MultiPolygon", "LineString", "MultiLineString"
        );
        root.put("type", names.get(geometry.getCoordinatesCase().getNumber() - 1));
        final HashMap<Integer, Function<GeoLocationGeometry, Collection>> serializers = new HashMap<>();
        serializers.put(1, g -> serialize(g.getCoordinatesPoint()));
        serializers.put(2, g -> serialize(g.getCoordinatesMultiPoint()));
        serializers.put(3, g -> serialize(g.getCoordinatesPolygon()));
        serializers.put(4, g -> serialize(g.getCoordinatesMultiPolygon()));
        serializers.put(5, g -> serialize(g.getCoordinatesLineString()));
        serializers.put(6, g -> serialize(g.getCoordinatesMultiLine()));
        root.put("coordinates", serializers.get(geometry.getCoordinatesCase().getNumber()).apply(geometry));
        return root;
    }

    private static Collection<Double> serialize(PointCoordinates points) {
        return points.getCoordinatesList();
    }

    private static Collection<Collection<Double>> serialize(MultiPointCoordinates points) {
        return points.getCoordinatesList().stream().map(FileParser::serialize).collect(Collectors.toList());
    }

    private static Collection<Collection<Collection<Double>>> serialize(PolygonCoordinates polygon) {
        return polygon.getCoordinatesList().stream().map(FileParser::serialize).collect(Collectors.toList());
    }

    private static Collection<Collection<Collection<Collection<Double>>>> serialize(MultiPolygonCoordinates polygon) {
        return polygon.getCoordinatesList().stream().map(FileParser::serialize).collect(Collectors.toList());
    }

    private static Collection<Collection<Double>> serialize(LineCoordinates line) {
        return line.getCoordinatesList().stream().map(FileParser::serialize).collect(Collectors.toList());
    }

    private static Collection<Collection<Collection<Double>>> serialize(MultiLineCoordinates multiline) {
        return multiline.getCoordinatesList().stream().map(FileParser::serialize).collect(Collectors.toList());
    }

    /**
     * Builds a request update item object from <code>FileMetadata</code>.
     *
     * An update item object updates an existing file (metadata) object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();


        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        // the parameter "name" cannot be updated.
        if (element.hasDirectory()) {
            updateNodeBuilder.put("directory", ImmutableMap.of("set", element.getDirectory()));
        }
        if (element.hasMimeType()) {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("set", element.getMimeType()));
        }
        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        }
        if (element.hasSourceCreatedTime()) {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("set", element.getSourceCreatedTime()));
        }
        if (element.hasSourceModifiedTime()) {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("set", element.getSourceModifiedTime()));
        }
        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }
        if (element.hasGeoLocation()) {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("set", getGeoLocationJson(element.getGeoLocation())));
        }

        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            updateNodeBuilder.put("labels", ImmutableMap.of("add", labels));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>FileMetadata</code>.
     *
     * A replace item object replaces an existing file (metadata) object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        // the parameter "name" cannot be updated.
        if (element.hasDirectory()) {
            updateNodeBuilder.put("directory", ImmutableMap.of("set", element.getDirectory()));
        } else {
            updateNodeBuilder.put("directory", ImmutableMap.of("setNull", true));
        }

        if (element.hasMimeType()) {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("set", element.getMimeType()));
        } else {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("setNull", true));
        }

        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        } else {
            updateNodeBuilder.put("source", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceCreatedTime()) {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("set", element.getSourceCreatedTime()));
        } else {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceModifiedTime()) {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("set", element.getSourceModifiedTime()));
        } else {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("setNull", true));
        }

        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        } else {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", ImmutableList.<Long>of()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        if (element.hasGeoLocation()) {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("set", getGeoLocationJson(element.getGeoLocation())));
        } else {
            updateNodeBuilder.put("geoLocation", ImmutableMap.of("setNull", true));
        }

        List<Map<String, String>> labels = new ArrayList<>();
        for (String label : element.getLabelsList()) {
            labels.add(ImmutableMap.of("externalId", label));
        }
        updateNodeBuilder.put("labels", ImmutableMap.of("set", labels));

        mapBuilder.put("update", updateNodeBuilder.build());

        return mapBuilder.build();
    }

    /**
     * Builds a request add assetId item object from <code>FileMetadata</code>.
     *
     * This method creates a special purpose item for adding assetIds to an existing file item. It is used for
     * posting files with very a very high number of assetIds. The Cognite API has a limit of 1k assetIds per request
     * so large assetId arrays need to be split into multiple update requests.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestAddAssetIdsItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();


        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("add", element.getAssetIdsList()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    private static String buildErrorMessage(String fieldName, String inputElement) {
        return logPrefix + "Unable to parse attribute: " + fieldName + ". Item exerpt: "
                + inputElement.substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH));
    }
}
