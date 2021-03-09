package com.cognite.client;

import com.google.common.base.Strings;

/**
 * Utility class for setting the client configuration for test runs.
 */
public class TestConfigProvider {

    public static String getProject() {
        String project = System.getenv("TEST_PROJECT");

        if (Strings.isNullOrEmpty(project)) {
            project = "test";
        }

        return project;
    }

    public static String getApiKey() {
        String apiKey = System.getenv("TEST_KEY");

        if (Strings.isNullOrEmpty(apiKey)) {
            apiKey = "test";
        }

        return apiKey;
    }

    public static String getHost() {
        String host = System.getenv("TEST_HOST");

        if (Strings.isNullOrEmpty(host)) {
            host = "http://localhost:4567";
        }
        return host;
    }
}
