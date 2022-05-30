## Geolocation

Geolocation is a special type of feature/attribute that can be attached to an object. Currently, the `file` resource type supports geolocation.

Geolocation represents a geographic feature, for example `point/position`, `line string` and `polygon`. The Wikipedia page on [GeoJSON](https://en.wikipedia.org/wiki/GeoJSON) offers a nice and short overview of the common geometry types.

Geographic geometries may be cumbersome to work with, so the SDK provides a set of utility classes in the `com.cognite.client.util.geo` package to offer added convenience.

```java
// Build a point from a set of coordinates
Point point = Points.of(12.0, 45.3)         // coordinates offered in lon, lat

// Build a line string from a set of coordinates
double[][] coordinates = {{10.0, 15.0}, {15.0, 20.0}, {16.0, 15.0}, {10.0, 15.0}};
LineString lineString = LineStrings.of(coordinates);

// Build a polygon from a set of coordinates
double[][][] coordinates ={
        {{10.0,15.0},{15.0,20.0},{16.0,15.0},{10.0,15.0}}
        };
Polygon polygon = Polygons.of(coordinates);

// Build a feature
Feature geoFeature = Feature.newBuilder()
        .setGeometry(Geometries.of(polygon))
        .setProperties(Structs.of(
                "type", Values.of("my source"),
                sourceKey, Values.of(12345))
        .build();
```