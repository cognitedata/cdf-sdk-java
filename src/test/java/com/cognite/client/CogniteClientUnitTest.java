package com.cognite.client;

import okhttp3.ConnectionSpec;
import okhttp3.Interceptor;
import org.junit.jupiter.api.Test;
import java.net.URL;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CogniteClientUnitTest {
    @Test
    void test_ofClientCredentials_config() throws Exception {
        CogniteClient client = CogniteClient.ofClientCredentials("myCdfProject", "123", "secret", new URL("https://localhost/cogniteapi"))
                .withBaseUrl("https://localhost");
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertDoesNotThrow(() -> client.buildAuthConfig());
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ClientCredentialsInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost", client.getBaseUrl());
        assertEquals(List.of("https://localhost/.default"), client.getAuthScopes());
    }

    @Test
    void test_ofClientCredentials_config_with_trailing_slash_in_baseUrl() throws Exception {
        CogniteClient client = CogniteClient.ofClientCredentials("myCdfProject", "123", "secret", new URL("https://localhost/cogniteapi"))
            .withBaseUrl("https://localhost/");
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertDoesNotThrow(() -> client.buildAuthConfig());
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ClientCredentialsInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost/", client.getBaseUrl());
        assertEquals(List.of("https://localhost/.default"), client.getAuthScopes());
    }

    @Test
    void test_withHttp_config() throws Exception {
        CogniteClient client = CogniteClient.ofClientCredentials("myCdfProject", "123", "secret", new URL("https://localhost/cogniteapi"))
                .withBaseUrl("https://localhost")
                .enableHttp(true);
        assertNotNull(client.getHttpClient());
        List<Interceptor> interceptorList = client.getHttpClient().interceptors();
        assertNotNull(interceptorList);
        assertEquals(1, interceptorList.size());
        assertEquals("com.cognite.client.CogniteClient$ClientCredentialsInterceptor", interceptorList.get(0).getClass().getName());
        assertEquals("https://localhost", client.getBaseUrl());
        List<ConnectionSpec> connectionSpecs = client.getHttpClient().connectionSpecs();
        assertEquals(3, connectionSpecs.size());
    }
}
