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

import com.cognite.client.dto.geo.MultiPoint;
import com.cognite.client.dto.geo.Point;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for working with {@link MultiPoint} objects.
 */
public class MultiPoints {

    /**
     * Create a {@link MultiPoint} object based on a set of geo {@link Point}s.
     *
     * @param points The set of Points representing the MultiPoint.
     * @return The {@link MultiPoint} representing the geo feature.
     */
    public static MultiPoint of(Point... points) {
        return MultiPoint.newBuilder()
                .addAllCoordinates(List.of(points))
                .build();
    }

    /**
     * Create a {@link MultiPoint} object based on a set of geo {@link Point}s.
     *
     * @param points The set of Points representing the MultiPoint.
     * @return The {@link MultiPoint} representing the geo feature.
     */
    public static MultiPoint of(List<Point> points) {
        return MultiPoint.newBuilder()
                .addAllCoordinates(points)
                .build();
    }

    /**
     * Create a {@link MultiPoint} object based on a list of point coordinates.
     *
     * Each entry in the list is a list of coordinate pairs or a triplet: long, lat and an optional elevation. The coordinate
     * datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees. The optional
     * elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * For example, {@code [[30.0, 40.0], [35.0, 45.0]]}.
     *
     * @param coordinates The coordinate pairs/triplets for the MultiPoint.
     * @return The {@link MultiPoint} representing the geo feature.
     */
    public static MultiPoint of(double[][] coordinates) throws Exception {
        List<Point> points = new ArrayList<>();
        for (double[] entry : coordinates) {
            points.add(Points.of(entry));
        }

        return of(points);
    }

    /**
     * Create a {@link MultiPoint} object based on a set of coordinates.
     *
     * The coordinates must be in pairs of long, lat. I.e. this method does not accept coordinates that contain an elevation
     * component, and you must supply an even number of coordinates in order for the input to be valid. The coordinate
     * datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     *
     * For example, the input array {@code [30.0, 40.0, 35.0, 45.0]} will be interpreted as the coordinate pairs of
     * {@code [[30.0, 40.0], [35.0, 45.0]]}.
     *
     * @param coordinates The coordinate pairs for the MultiPoint.
     * @return The {@link MultiPoint} representing the geo feature.
     */
    public static MultiPoint of(double... coordinates) throws Exception {
        Preconditions.checkArgument(coordinates.length % 2 == 0,
                String.format("You must supply an even number of coordinates. Input length: %d", coordinates.length));
        List<double[]> coordinatePairs = new ArrayList<>();
        for (int i = 0; i < (coordinates.length - 1); i += 2) {
            double[] pair = {coordinates[i], coordinates[i + 1]};
            coordinatePairs.add(pair);
        }

        return of(coordinatePairs.toArray(new double[0][]));
    }
}
