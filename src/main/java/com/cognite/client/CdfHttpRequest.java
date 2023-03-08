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

package com.cognite.client;

import com.google.api.Http;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class allows you to make a HTTP(S) request to an arbitrary Cognite Data Fusion API endpoint.
 *
 *
 */
@AutoValue
public abstract class CdfHttpRequest extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(CdfHttpRequest.class);

    protected final Map<String, String> headers = new HashMap<>();

    private static Builder builder() {
        return new AutoValue_CdfHttpRequest.Builder();
    }

    /**
     * Construct a new {@link CdfHttpRequest} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static CdfHttpRequest of(CogniteClient client, String apiPath) {
        return CdfHttpRequest.builder()
                .setClient(client)
                .setApiPath(apiPath)
                .build();
    }

    abstract CdfHttpRequest.Builder toBuilder();
    abstract String getApiPath();
    @Nullable
    abstract Request getRequestBody();

    public CdfHttpRequest withHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }


    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Builder setApiPath(String value);
        abstract Builder setRequestBody(Request value);

        abstract CdfHttpRequest build();
    }
}
