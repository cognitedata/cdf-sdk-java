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

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.DataSetParser;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class allows you to make a HTTP(S) request to an arbitrary path.
 *
 *
 */
@AutoValue
public abstract class CdfHttpRequest extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(CdfHttpRequest.class);

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
    public static CdfHttpRequest of(CogniteClient client) {
        return CdfHttpRequest.builder()
                .setClient(client)
                .build();
    }


    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract CdfHttpRequest build();
    }
}
