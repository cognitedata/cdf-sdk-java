package com.cognite.client;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CogniteClientTest {

    @Test
    /** Expects java.net.UnknownServiceException: CLEARTEXT communication not enabled for client as basic HTTP is not supported */
    void test_block_http_URI() throws Exception {
        Exception exception = assertThrows(java.net.UnknownServiceException.class, () -> {
            try {
                CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("http://localhost");
                Iterator<List<String>> databases = client.raw().databases().list();
            }catch (java.util.concurrent.CompletionException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    /** Expects connection refused (no localhost exist) */
    void test_accept_https_URI() throws Exception {
        Exception exception = assertThrows(java.net.ConnectException.class, () -> {
            try {
                CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("https://localhost");
                Iterator<List<String>> databases = client.raw().databases().list();
            }catch (java.util.concurrent.CompletionException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    /** Expects connection refused (no localhost exist) */
    @SetEnvironmentVariable(key = "enableCdfOverHttp", value = "true")
    void test_accept_http_URI_withFlag() throws Exception {
        //System.setProperty("enableCdfOverHttp", Boolean.TRUE.toString());
        Exception exception = assertThrows(java.net.ConnectException.class, () -> {
            try {
                CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("http://localhost");
                Iterator<List<String>> databases = client.raw().databases().list();
            }catch (java.util.concurrent.CompletionException e) {
                throw e.getCause();
            }
        });
        System.getProperties().remove("enableCdfOverHttp");
    }
}