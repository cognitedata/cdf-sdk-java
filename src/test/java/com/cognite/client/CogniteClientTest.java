package com.cognite.client;

import okhttp3.ConnectionSpec;
import okhttp3.Interceptor;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.net.URL;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CogniteClientTest {

    @Test
    void test_ofKey_config() throws Exception {
        CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("https://localhost");
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ApiKeyInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("TEST", client.getApiKey());
        assertEquals("https://localhost", client.getBaseUrl());
    }

    @Test
    void test_ofClientCredentials_config() throws Exception {
        CogniteClient client = CogniteClient.ofClientCredentials("123", "secret", new URL("https://localhost/cogniteapi")).withBaseUrl("https://localhost");
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ClientCredentialsInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost", client.getBaseUrl());
    }


    @Test
    void test_withHttp_config() throws Exception {
        CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("https://localhost").withHttp();
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ApiKeyInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost", client.getBaseUrl());
        List<ConnectionSpec> connectionSpecs = client.getHttpClient().connectionSpecs();
        assertEquals(3, connectionSpecs.size());
    }
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
    void test_accept_http_URI_withFlag() throws Exception {
        Exception exception = assertThrows(java.net.ConnectException.class, () -> {
            try {
                CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("http://localhost").withHttp();
                Iterator<List<String>> databases = client.raw().databases().list();
            }catch (java.util.concurrent.CompletionException e) {
                throw e.getCause();
            }
        });
    }
}