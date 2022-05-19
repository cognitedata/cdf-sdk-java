package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.*;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.cognite.client.servicesV1.parser.FileParser.getGeoLocationJson;
import static com.cognite.client.servicesV1.parser.FileParser.parseFileMetadata;
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
                        .setCoordinatesPoint(p1)
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
                        .setCoordinatesMultiPolygon(mpoly)
                        .build())
                .build();
        final Map<String, Object> map = getGeoLocationJson(defaultInstance);
        assertEquals("Feature", map.get("type"));
        assertEquals("MultiPolygon", ((Map<String, Object>) map.get("geometry")).get("type"));
        assertEquals("[[[[0.5,0.3],[0.3,0.5]],[[0.5,0.3],[0.3,0.5]]],[[[0.5,0.3],[0.3,0.5]],[[0.5,0.3],[0.3,0.5]]]]", ((Map<String, Object>) map.get("geometry")).get("coordinates"));
    }

    @Test
    public void testPointDesirialization() throws Exception {
        String fileMetadata = "{\n" +
                "  \"externalId\": \"my.known.id\",\n" +
                "  \"name\": \"string\",\n" +
                "  \"directory\": \"string\",\n" +
                "  \"source\": \"string\",\n" +
                "  \"mimeType\": \"image/jpeg\",\n" +
                "  \"metadata\": {\n" +
                "    \"property1\": \"string\",\n" +
                "    \"property2\": \"string\"\n" +
                "  },\n" +
                "  \"assetIds\": [\n" +
                "    1\n" +
                "  ],\n" +
                "  \"dataSetId\": 1,\n" +
                "  \"sourceCreatedTime\": 0,\n" +
                "  \"sourceModifiedTime\": 0,\n" +
                "  \"securityCategories\": [\n" +
                "    1\n" +
                "  ],\n" +
                "  \"labels\": [\n" +
                "    {\n" +
                "      \"externalId\": \"my.known.id\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"geoLocation\": {\n" +
                "    \"type\": \"Feature\",\n" +
                "    \"geometry\": {\n" +
                "      \"type\": \"MultiPolygon\",\n" +
                "      \"coordinates\": [[[[30, 20], [45, 40], [10, 40], [30, 20]]], [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]]\n" +
                "    },\n" +
                "    \"properties\": {}\n" +
                "  },\n" +
                "  \"id\": 1,\n" +
                "  \"uploaded\": true,\n" +
                "  \"uploadedTime\": 0,\n" +
                "  \"createdTime\": 0,\n" +
                "  \"lastUpdatedTime\": 0,\n" +
                "  \"uploadUrl\": \"string\"\n" +
                "}";
        FileMetadata f = parseFileMetadata(fileMetadata);
        assertEquals("Feature", f.getGeoLocation().getType());
        assertEquals(2, f.getGeoLocation().getGeometry().getCoordinatesMultiPolygon().getCoordinatesCount());
    }

}