/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

abstract class GenericPlaygroundRequestProvider implements RequestProvider, Serializable {
    protected static final String apiVersion = "playground";

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    // Logger identifier per instance
    protected final String randomIdString = RandomStringUtils.randomAlphanumeric(5);

    public abstract String getSdkIdentifier();
    public abstract String getAppIdentifier();
    public abstract String getSessionIdentifier();
    public abstract String getEndpoint();
    public abstract Request getRequest();

    protected okhttp3.Request.Builder buildGenericRequest() throws URISyntaxException {
        Preconditions.checkState(this.getAppIdentifier().length() < 40
                , "App identifier out of range. Length must be < 40.");
        Preconditions.checkState(this.getSdkIdentifier().length() < 40
                , "SDK identifier out of range. Length must be < 40.");
        Preconditions.checkState(this.getSessionIdentifier().length() < 40
                , "Session identifier out of range. Length must be < 40.");

        // build standard part of the request.
        return new okhttp3.Request.Builder()
                .header("Accept", "application/json")
                .header("x-cdp-sdk", this.getSdkIdentifier())
                .header("x-cdp-app", this.getAppIdentifier())
                .header("x-cdp-clienttag", this.getSessionIdentifier())
                .url(buildGenericUrl().build());
    }

    protected HttpUrl.Builder buildGenericUrl() throws URISyntaxException {
        URI uri = new URI(this.getRequest().getAuthConfig().getHost());
        return new HttpUrl.Builder()
                .scheme(uri.getScheme())
                .host(uri.getHost())
                .addPathSegment("api")
                .addPathSegment(apiVersion)
                .addPathSegment("projects")
                .addPathSegment(this.getRequest().getAuthConfig().getProject())
                .addPathSegments(this.getEndpoint());
    }

    abstract static class Builder<B extends com.cognite.client.servicesV1.request.GenericPlaygroundRequestProvider.Builder<B>> {
        public abstract B setSdkIdentifier(String value);
        public abstract B setAppIdentifier(String value);
        public abstract B setSessionIdentifier(String value);
        public abstract B setEndpoint(String value);
        public abstract B setRequest(Request value);
    }
}
