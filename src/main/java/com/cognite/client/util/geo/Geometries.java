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

import com.cognite.client.dto.geo.*;

/**
 * Helper class for working with {@link Geometry} objects.
 */
public class Geometries {

    /**
     * Create a {@link Geometry} object with {@link Point} coordinates.
     *
     * @param coordinates The point coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(Point coordinates) {
        return Geometry.newBuilder()
                .setPoint(coordinates)
                .build();
    }

    /**
     * Create a {@link Geometry} object with {@link LineString} coordinates.
     *
     * @param coordinates The line string coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(LineString coordinates) {
        return Geometry.newBuilder()
                .setLineString(coordinates)
                .build();
    }

    /**
     * Create a {@link Geometry} object with {@link Polygon} coordinates.
     *
     * @param coordinates The polygon coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(Polygon coordinates) {
        return Geometry.newBuilder()
                .setPolygon(coordinates)
                .build();
    }

    /**
     * Create a {@link Geometry} object with {@link MultiPoint} coordinates.
     *
     * @param coordinates The multi point coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(MultiPoint coordinates) {
        return Geometry.newBuilder()
                .setMultiPoint(coordinates)
                .build();
    }

    /**
     * Create a {@link Geometry} object with {@link MultiLineString} coordinates.
     *
     * @param coordinates The multi line string coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(MultiLineString coordinates) {
        return Geometry.newBuilder()
                .setMultiLineString(coordinates)
                .build();
    }

    /**
     * Create a {@link Geometry} object with {@link MultiPolygon} coordinates.
     *
     * @param coordinates The multi polygon coordinates of the geometry.
     * @return The geometry with the coordinates configured.
     */
    public static Geometry of(MultiPolygon coordinates) {
        return Geometry.newBuilder()
                .setMultiPolygon(coordinates)
                .build();
    }
}
