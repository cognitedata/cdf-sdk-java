package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.google.common.base.Strings;

import java.net.MalformedURLException;

/**
 * Utility class for setting the client configuration for test runs.
 */
public class TestConfigProvider {
    public static CogniteClient getCogniteClient() throws MalformedURLException {
        return CogniteClient.ofClientCredentials(
                        TestConfigProvider.getProject(),
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withBaseUrl(TestConfigProvider.getHost());
    }

    public static CogniteClient getOpenIndustrialDataCogniteClient() throws MalformedURLException {
        return CogniteClient.ofClientCredentials(
                TestConfigProvider.getOpenIndustrialDataProject(),
                TestConfigProvider.getOpenIndustrialDataClientId(),
                TestConfigProvider.getOpenIndustrialDataClientSecret(),
                TokenUrl.generateAzureAdURL(TestConfigProvider.getOpenIndustrialDataTenantId())
        );
    }

    public static String getOpenIndustrialDataProject() {
        return "publicdata";
    }

    public static String getOpenIndustrialDataClientId() {
        return "1b90ede3-271e-401b-81a0-a4d52bea3273"; // public info
    }

    public static String getOpenIndustrialDataClientSecret() {
        String clientSecret = System.getenv("OPEN_IND_DATA_CLIENT_SECRET");
        if (Strings.isNullOrEmpty(clientSecret)) {
            clientSecret = "test";
        }
        return clientSecret;
    }

    public static String getOpenIndustrialDataTenantId() {
        return "48d5043c-cf70-4c49-881c-c638f5796997"; // public info
    }

    public static String getProject() {
        String project = System.getenv("TEST_PROJECT");

        if (Strings.isNullOrEmpty(project)) {
            project = "test";
        }

        return project;
    }

    public static String getClientId() {
        String clientId = System.getenv("TEST_CLIENT_ID");

        if (Strings.isNullOrEmpty(clientId)) {
            clientId = "default";
        }
        return clientId;
    }

    public static String getClientSecret() {
        String clientSecret = System.getenv("TEST_CLIENT_SECRET");

        if (Strings.isNullOrEmpty(clientSecret)) {
            clientSecret = "default";
        }
        return clientSecret;
    }

    public static String getTenantId() {
        String tenantId = System.getenv("TEST_TENANT_ID");

        if (Strings.isNullOrEmpty(tenantId)) {
            tenantId = "default";
        }
        return tenantId;
    }

    public static String getHost() {
        String host = System.getenv("TEST_HOST");

        if (Strings.isNullOrEmpty(host)) {
            host = "http://localhost:4567";
        }
        return host;
    }
}
