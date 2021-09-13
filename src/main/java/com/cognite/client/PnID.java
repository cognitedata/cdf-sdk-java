/*
 * Copyright (c) 2021 Cognite AS
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

import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the Cognite P&ID api endpoint
 *
 * It provides methods for scanning and parsing P&IDs.
 */
@AutoValue
public abstract class PnID extends ApiBase {

    private static Builder builder() {
        return new AutoValue_PnID.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(PnID.class);

    /**
     * Construct a new {@link PnID} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static PnID of(CogniteClient client) {
        return PnID.builder()
                .setClient(client)
                .build();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract PnID build();
    }
}
