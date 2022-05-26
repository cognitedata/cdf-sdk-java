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

import com.cognite.client.dto.geo.Point;

/**
 * Helper class for working with {@link Point} objects.
 */
public class Points {

    /**
     * Create a {@link Point} object based on longitude (easting) and latitude (northing) coordinates.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     *
     * @param lon The longitude decimal degrees value of the point.
     * @param lat The latitude decimal degrees value of the point.
     * @return The {@link Point} representing the geo location.
     */
    public static Point of(double lon, double lat) {
        return Point.newBuilder()
                .setLon(lon)
                .setLat(lat)
                .build();
    }

    /**
     * Create a {@link Point} object based on longitude (easting) and latitude (northing) coordinates.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     * The elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * @param lon The longitude decimal degrees value of the point.
     * @param lat The latitude decimal degrees value of the point.
     * @param elevation The elevation in meters.
     * @return The {@link Point} representing the geo location.
     */
    public static Point of(double lon, double lat, double elevation) {
        return Point.newBuilder()
                .setLon(lon)
                .setLat(lat)
                .setElev(elevation)
                .build();
    }
}
