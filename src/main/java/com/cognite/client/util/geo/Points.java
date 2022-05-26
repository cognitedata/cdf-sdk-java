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
import com.google.common.base.Preconditions;

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
     * @return The {@link Point} representing the geolocation.
     */
    public static Point of(double lon, double lat) {
        return Point.newBuilder()
                .setLon(lon)
                .setLat(lat)
                .build();
    }

    /**
     * Create a {@link Point} object based on longitude (easting), latitude (northing) and elevation coordinates.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     * The elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * @param lon The longitude decimal degrees value of the point.
     * @param lat The latitude decimal degrees value of the point.
     * @param elevation The elevation in meters.
     * @return The {@link Point} representing the geolocation.
     */
    public static Point of(double lon, double lat, double elevation) {
        return Point.newBuilder()
                .setLon(lon)
                .setLat(lat)
                .setElev(elevation)
                .build();
    }

    /**
     * Create a {@link Point} object based on longitude (easting), latitude (northing) and (optional) elevation coordinates.
     *
     * The coordinate datum is World Geodetic System 1984 (WGS 84), with longitude and latitude units of decimal degrees.
     * The elevation is expressed in height in meters above or below the WGS 84 reference ellipsoid.
     *
     * The input arguments must be supplied in the order of longitude (1st), latitude (2nd) and elevation (3rd). The
     * elevation component is optional. I.e. you must supply minimum 2 input arguments.
     *
     * @param coordinates The long, lat and (optional) elevation of the point.
     * @return The {@link Point} representing the geolocation.
     */
    public static Point of(double[] coordinates) throws Exception {
        Preconditions.checkState(coordinates.length > 1,
                String.format("Each point entry must contain minimum 2 coordinates. Invalid entry: %s", coordinates));

        if (coordinates.length > 2) {
            return of(coordinates[0], coordinates[1], coordinates[2]);
        } else {
            return of(coordinates[0], coordinates[1]);
        }
    }
}
