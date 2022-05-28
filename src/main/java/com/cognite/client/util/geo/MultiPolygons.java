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

package com.cognite.client.util.geo;

import com.cognite.client.dto.geo.LineString;
import com.cognite.client.dto.geo.MultiPolygon;
import com.cognite.client.dto.geo.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for working with {@link MultiPolygon} objects.
 */
public class MultiPolygons {

    /**
     * Create a {@link MultiPolygon} object based on a set of geo {@link Polygon}s.
     *
     * @param polygons The set of LineStrings representing the Polygon.
     * @return The {@link MultiPolygon} representing the geo feature.
     */
    public static MultiPolygon of(Polygon... polygons) {
        return MultiPolygon.newBuilder()
                .addAllCoordinates(List.of(polygons))
                .build();
    }

    /**
     * Create a {@link MultiPolygon} object based on a set of geo {@link Polygon}s.
     *
     * @param polygons The set of LineStrings representing the Polygon.
     * @return The {@link MultiPolygon} representing the geo feature.
     */
    public static MultiPolygon of(List<Polygon> polygons) {
        return MultiPolygon.newBuilder()
                .addAllCoordinates(polygons)
                .build();
    }

    /**
     * Create a {@link MultiPolygon} object based on a list of polygon point coordinates.
     *
     * Each entry in the list is a list of list of positions (coordinate pairs or a triplet): long, lat and an optional elevation.
     * I.e. a list of polygon coordinates.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     * The optional elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * For example, {@code [
     * [[[30.0, 40.0], [35.0, 45.0], [35.0, 40.0], [30.0, 40.0]]],
     * [[[10.0, 5.0], [25.0, 25.0], [15.0, 10.0], [10.0, 5.0]]]
     * ]}.
     *
     * @param coordinates The coordinates for the MultiPolygon.
     * @return The {@link MultiPolygon} representing the geo feature.
     */
    public static MultiPolygon of(double[][][][] coordinates) throws Exception {
        List<Polygon> polygons = new ArrayList<>();
        for (double[][][] polygon : coordinates) {
            polygons.add(Polygons.of(polygon));
        }

        return of(polygons);
    }
}
