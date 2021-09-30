# Unit testing with wiremock quick howto
Enabling running wiremock using plain http, the environment variable "enableCdfOverHttp" 
must be set to true. This can be done for example by using junit-pioneer @SetEnviromentVariable
annotation.<br>
**Note:** Currently only works with port 80!

## Example

### POM
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>MyTest</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.cognite</groupId>
            <artifactId>cdf-sdk-java</artifactId>
            <version>1.3.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>com.github.tomakehurst</groupId>
            <artifactId>wiremock-jre8</artifactId>
            <version>2.31.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

</project>
```

### Junit java class
```java
import com.cognite.client.CogniteClient;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import java.util.Iterator;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest(httpPort = 80)
public class TestCdfMock {

    @Test
    public void testCdfConnect(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        stubFor(get("/login/status").willReturn(ok().withBody("{\n" +
                "    \"data\": {\n" +
                "        \"user\": \"some.user@cognite.com\",\n" +
                "        \"loggedIn\": true,\n" +
                "        \"project\": \"dev\",\n" +
                "        \"projectId\": 123456789,\n" +
                "        \"apiKeyId\": 987654321\n" +
                "    }\n" +
                "}")));

        stubFor(get(urlPathEqualTo("/api/v1/projects/dev/raw/dbs")).willReturn(ok().withBody("{\n" +
                "    \"items\": [\n" +
                "        {\n" +
                "            \"name\": \"MY_DB\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"nextCursor\": \"wIxxsu58VLnXfHvBQL8v3g==\"\n" +
                "}")));

        CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("http://localhost/").enableHttp(true);
        Iterator<List<String>> databases = client.raw().databases().list();
        assertTrue(databases.hasNext());
        assertEquals(1, databases.next().size());
    }
}
```