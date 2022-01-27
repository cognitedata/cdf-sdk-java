package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

class ThreeDFilesIntegrationTest {
    private static final Long _3D_FILE_ID = 4496458695968385L;

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void read3DFile() throws Throwable {
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - read3DFile() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = this.getTestCogniteClient();

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start downloading 3D file binary.");

        Assertions.assertNotNull(_3D_FILE_ID, "To run the 3d File download test you need to provide a 3d File id.");

        client.threeD().files().downloadToPath(_3D_FILE_ID, Paths.get(""));

        LOG.info(loggingPrefix + "Finished download 3D file binary. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

    }

    private CogniteClient getTestCogniteClient() throws MalformedURLException {
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        return client;
    }

}