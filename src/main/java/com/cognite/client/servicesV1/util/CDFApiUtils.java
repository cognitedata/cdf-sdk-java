package com.cognite.client.servicesV1.util;

import com.cognite.client.Request;
import okhttp3.HttpUrl;

import java.net.URI;
import java.net.URISyntaxException;

public class CDFApiUtils {

    /**
     * Returns a HttpUrl.Builder pre-filled with the host address.
     *
     * @return HttpUrl.Builder
     * @throws URISyntaxException
     */
    public static HttpUrl.Builder defaultHostURLBuilder(final Request request, final String endpoint) throws URISyntaxException {
        URI uri = new URI(request.getAuthConfig().getHost());
        return new HttpUrl.Builder()
                .scheme(uri.getScheme())
                .host(uri.getHost());
    }

    /**
     * Returns a HttpUrl.Builder pre-filled with the host address, the resource and the endpoint for the given inner parameters.
     *
     * @return HttpUrl.Builder
     * @throws URISyntaxException
     */
    public static HttpUrl.Builder defaultAPIEndpointBuilder(final Request request, final String endpoint, final String apiVersion) throws URISyntaxException {
        return defaultHostURLBuilder(request, endpoint)
                .addPathSegment("api")
                .addPathSegment(apiVersion)
                .addPathSegment("projects")
                .addPathSegment(request.getAuthConfig().getProject())
                .addPathSegments(endpoint);
    }
}
