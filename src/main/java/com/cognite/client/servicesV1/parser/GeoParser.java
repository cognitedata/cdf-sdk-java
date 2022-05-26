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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
     * @throws Exception
     */
    public static Map<String, Object> parseFeatureToMap(Feature element) throws Exception {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Fixed string that always accompanies a Feature
        mapBuilder.put("type", "Feature");

        if (element.hasGeometry()) {
            Geometry geometry = element.getGeometry();

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
