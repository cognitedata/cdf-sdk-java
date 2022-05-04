package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.*;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.cognite.client.servicesV1.parser.FileParser.getGeoLocationJson;
import static org.junit.jupiter.api.Assertions.assertEquals;


class FileParserGeoLocationTest {

    private static PointCoordinates p1 = PointCoordinates.newBuilder()
            .addAllCoordinates(ImmutableList.of(0.5, 0.3))
            .build();

    private static PointCoordinates p2 = PointCoordinates.newBuilder()
            .addAllCoordinates(ImmutableList.of(0.3, 0.5))
            .build();

    private static MultiPointCoordinates mp1 = MultiPointCoordinates.newBuilder()
            .addAllCoordinates(ImmutableList.of(p1, p2))
            .build();

    private static PolygonCoordinates poly = PolygonCoordinates.newBuilder()
            .addAllCoordinates(ImmutableList.of(mp1, mp1))
            .build();

    private static MultiPolygonCoordinates mpoly = MultiPolygonCoordinates.newBuilder()
            .addAllCoordinates(ImmutableList.of(poly, poly))
            .build();


    @Test
    public void testPointGeoLocationSerialization() {
        final GeoLocation defaultInstance = GeoLocation.getDefaultInstance().toBuilder()
                .setType("Feature")
                .setGeometry(GeoLocationGeometry.newBuilder()
                        .setType("Point")
                        .setCoordinatesPoints(p1)
                        .build())
                .build();
        final Map<String, Object> map = getGeoLocationJson(defaultInstance);
        assertEquals("Feature", map.get("type"));
        assertEquals("Point", ((Map<String, Object>) map.get("geometry")).get("type"));
        assertEquals("[0.5,0.3]", ((Map<String, Object>) map.get("geometry")).get("coordinates"));
    }

    @Test
    public void testMultiPointGeoLocationSerialization() {
        final GeoLocation defaultInstance = GeoLocation.getDefaultInstance().toBuilder()
                .setType("Feature")
                .setGeometry(GeoLocationGeometry.newBuilder()
                        .setType("MultiPoint")
                        .setCoordinatesMultiPoint(mp1)
                        .build())
                .build();
        final Map<String, Object> map = getGeoLocationJson(defaultInstance);
        assertEquals("Feature", map.get("type"));
        assertEquals("MultiPoint", ((Map<String, Object>) map.get("geometry")).get("type"));
        assertEquals("[[0.5,0.3],[0.3,0.5]]", ((Map<String, Object>) map.get("geometry")).get("coordinates"));
    }

    @Test
    public void testMultiPolygonGeoLocationSerialization() {
        final GeoLocation defaultInstance = GeoLocation.getDefaultInstance().toBuilder()
                .setType("Feature")
                .setGeometry(GeoLocationGeometry.newBuilder()
                        .setType("MultiPolygon")
                        .setCoordinatesMultiPolygon(mpoly)
                        .build())
                .build();
        final Map<String, Object> map = getGeoLocationJson(defaultInstance);
        assertEquals("Feature", map.get("type"));
        assertEquals("MultiPolygon", ((Map<String, Object>) map.get("geometry")).get("type"));
        assertEquals("[[[[0.5,0.3],[0.3,0.5]],[[0.5,0.3],[0.3,0.5]]],[[[0.5,0.3],[0.3,0.5]],[[0.5,0.3],[0.3,0.5]]]]", ((Map<String, Object>) map.get("geometry")).get("coordinates"));
    }

}