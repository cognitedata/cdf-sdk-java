package com.cognite.client;

import com.cognite.client.dto.LoginStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LoginTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void readLoginStatusByApiKey() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - readLoginStatusByApiKey() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey());

        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start get login status.");

        LoginStatus loginStatus = client.login().loginStatusByApiKey();

        LOG.info(loggingPrefix + "Finished get login status. Duration :  {}",
                Duration.between(startInstant, Instant.now()));

        assertNotNull(loginStatus.getApiKeyId());
        assertNotNull(loginStatus.getProject());
        assertNotNull(loginStatus.getLoggedIn());
        assertNotNull(loginStatus.getUser());
    }
}
