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
import com.cognite.client.dto.geo.MultiLineString;
import com.cognite.client.dto.geo.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for working with {@link MultiLineString} objects.
 */
public class MultiLineStrings {

    /**
     * Create a {@link MultiLineString} object based on a set of geo {@link LineString}s.
     *
     * @param lineStrings The set of LineStrings representing the MultiLineString.
     * @return The {@link MultiLineString} representing the geo feature.
     */
    public static MultiLineString of(LineString... lineStrings) {
        return MultiLineString.newBuilder()
                .addAllCoordinates(List.of(lineStrings))
                .build();
    }

    /**
     * Create a {@link MultiLineString} object based on a set of geo {@link LineString}s.
     *
     * @param lineStrings The set of LineStrings representing the MultiLineString.
     * @return The {@link MultiLineString} representing the geo feature.
     */
    public static MultiLineString of(List<LineString> lineStrings) {
        return MultiLineString.newBuilder()
                .addAllCoordinates(lineStrings)
                .build();
    }

    /**
     * Create a {@link MultiLineString} object based on a list of list of point coordinates.
     *
     * Each entry in the list is a list of positions (coordinate pairs or a triplet): long, lat and an optional elevation.
     * I.e. a list of line strings.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     * The optional elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * For example, {@code [[[30.0, 40.0], [35.0, 45.0], [35.0, 40.0]], [[10.0, 5.0], [15.0, 25.0]]]}.
     *
     * @param coordinates The coordinates for the MultiLineString.
     * @return The {@link MultiLineString} representing the geo feature.
     */
    public static MultiLineString of(double[][][] coordinates) throws Exception {
        List<LineString> lineStrings = new ArrayList<>();
        for (double[][] lineString : coordinates) {
            lineStrings.add(LineStrings.of(lineString));
        }

        return of(lineStrings);
    }
}
