package com.cognite.client;

import okhttp3.ConnectionSpec;
import okhttp3.Interceptor;
import org.checkerframework.framework.qual.IgnoreInWholeProgramInference;
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
        CogniteClient client = CogniteClient.ofKey("TEST").withBaseUrl("https://localhost").enableHttp(true);
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ApiKeyInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost", client.getBaseUrl());
        List<ConnectionSpec> connectionSpecs = client.getHttpClient().connectionSpecs();
        assertEquals(3, connectionSpecs.size());
    }
    
}