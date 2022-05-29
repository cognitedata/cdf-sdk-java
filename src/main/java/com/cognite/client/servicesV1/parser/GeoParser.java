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

import com.cognite.client.dto.geo.*;
import com.cognite.client.util.geo.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.*;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class GeoParser {
    static final String logPrefix = "GeoParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Builds a request object from a {@link Feature} dto.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> parseFeatureToMap(Feature element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Fixed string that always accompanies a Feature
        mapBuilder.put("type", "Feature");

        if (element.hasGeometry()
                && element.getGeometry().getGeometryTypeCase() != Geometry.GeometryTypeCase.GEOMETRYTYPE_NOT_SET) {
            Map<String, Object> geoMap = new HashMap<>();
            String geometryTypeKey = "type";
            String geometryCoordinatesKey = "coordinates";
            Geometry geometry = element.getGeometry();
            switch (geometry.getGeometryTypeCase()) {
                case POINT:
                    geoMap.put(geometryTypeKey, "Point");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getPoint()));
                    break;
                case LINE_STRING:
                    geoMap.put(geometryTypeKey, "LineString");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getLineString()));
                    break;
                case POLYGON:
                    geoMap.put(geometryTypeKey, "Polygon");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getPolygon()));
                    break;
                case MULTI_POINT:
                    geoMap.put(geometryTypeKey, "MultiPoint");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getMultiPoint()));
                    break;
                case MULTI_LINE_STRING:
                    geoMap.put(geometryTypeKey, "MultiLineString");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getMultiLineString()));
                    break;
                case MULTI_POLYGON:
                    geoMap.put(geometryTypeKey, "MultiPolygon");
                    geoMap.put(geometryCoordinatesKey, parseCoordinates(geometry.getMultiPolygon()));
                    break;
            }
            mapBuilder.put("geometry", geoMap);
        } else {
            // GeoJson specifies that the geometry attribute must be explicitly set to null if it is unallocated.
            mapBuilder.put("geometry", null);
        }

        if (element.hasProperties()) {
            mapBuilder.put("properties", StructParser.parseStructToMap(element.getProperties()));
        }

        return mapBuilder.build();
    }

    public static Feature parseFeature(String geoJson) throws Exception {
        Preconditions.checkNotNull(geoJson, "Input string cannot be null");
        String logItemExcerpt = geoJson.substring(0, Math.min(geoJson.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        JsonNode root = objectMapper.readTree(geoJson);
        Feature.Builder featureBuilder = Feature.newBuilder();

        // The Json object must contain an attribute "type" with the value "Feature"
        if (!(root.path("type").isTextual() && root.path("type").textValue().equals("Feature"))) {
            String message = String.format(logPrefix + "Unable to parse attribute: type. Item excerpt: %n%s",
                    logItemExcerpt);
            throw new Exception(message);
        }

        String coordinatesPath = "coordinates";
        if (root.path("geometry").isObject()
                && root.path("geometry").path("type").isTextual()
                && root.path("geometry").path(coordinatesPath).isArray()) {

            JsonNode geometry = root.path("geometry");
            String type = geometry.path("type").textValue();
            switch (type) {
                case "Point":
                    featureBuilder.setGeometry(Geometries.of(
                            Points.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[].class))));
                    break;
                case "LineString":
                    featureBuilder.setGeometry(Geometries.of(
                            LineStrings.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[][].class))));
                    break;
                case "Polygon":
                    featureBuilder.setGeometry(Geometries.of(
                            Polygons.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[][][].class))));
                    break;
                case "MultiPoint":
                    featureBuilder.setGeometry(Geometries.of(
                            MultiPoints.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[][].class))));
                    break;
                case "MultiLineString":
                    featureBuilder.setGeometry(Geometries.of(
                            MultiLineStrings.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[][][].class))));
                    break;
                case "MultiPolygon":
                    featureBuilder.setGeometry(Geometries.of(
                            MultiPolygons.of(objectMapper.treeToValue(geometry.path(coordinatesPath), double[][][][].class))));
                    break;
                default:
                    throw new Exception(String.format(logPrefix + "Invalid geometry type: %s.%n Json excerpt: %s",
                            type,
                            logItemExcerpt));
            }
        } else {
            String message = String.format(logPrefix + "Unable to parse attribute: geometry. Item excerpt: %n%s",
                    logItemExcerpt);
            throw new Exception(message);
        }

        if (root.path("properties").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(objectMapper.writeValueAsString(root.get("properties")), structBuilder);
            featureBuilder.setProperties(structBuilder.build());
        }

        return featureBuilder.build();
    }

    private static double[][][][] parseCoordinates(MultiPolygon multiPolygon) {
        List<double[][][]> positions = new ArrayList<>();
        for (Polygon polygon : multiPolygon.getCoordinatesList()) {
            positions.add(parseCoordinates(polygon));
        }
        return positions.toArray(new double[0][][][]);
    }

    private static double[][][] parseCoordinates(Polygon polygon) {
        return parseLineStringListCoordinates(polygon.getCoordinatesList());
    }

    private static double[][][] parseCoordinates(MultiLineString multiLineString) {
        return parseLineStringListCoordinates(multiLineString.getCoordinatesList());
    }

    private static double[][][] parseLineStringListCoordinates(List<LineString> lineStrings) {
        List<double[][]> positions = new ArrayList<>();
        for (LineString line : lineStrings) {
            positions.add(parseCoordinates(line));
        }
        return positions.toArray(new double[0][][]);
    }

    private static double[][] parseCoordinates(MultiPoint multiPoint) {
        return parsePointsListCoordinates(multiPoint.getCoordinatesList());
    }

    private static double[][] parseCoordinates(LineString lineString) {
        return parsePointsListCoordinates(lineString.getCoordinatesList());
    }

    private static double[][] parsePointsListCoordinates(List<Point> points) {
        List<double[]> positions = new ArrayList<>();
        for (Point point : points) {
            positions.add(parseCoordinates(point));
        }
        return positions.toArray(new double[0][]);
    }

    private static double[] parseCoordinates(Point point) {
        double[] coordinates = new double[2];
        if (point.hasElev()) {
            coordinates = new double[3];
            coordinates[2] = point.getElev();
        }
        coordinates[0] = point.getLon();
        coordinates[1] = point.getLat();

        return coordinates;
    }
}
