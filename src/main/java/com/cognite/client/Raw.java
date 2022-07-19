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

import com.google.auto.value.AutoValue;

/**
 * This class represents the Cognite raw api endpoint.
 *
 * It provides methods for interacting with the Raw service.
 */
@AutoValue
public abstract class Raw extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Raw.Builder();
    }

    /**
     * Constructs a new {@link Raw} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Raw of(CogniteClient client) {
        return Raw.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link RawDatabases} representing the Cognite Raw Tables api endpoint.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.raw().databases();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     *
     * @return The raw tables api object.
     */
    public RawDatabases databases() {
        return RawDatabases.of(getClient());
    }

    /**
     * Returns {@link RawTables} representing the Cognite Raw Tables api endpoint.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.raw().tables();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     *
     * @return The raw tables api object.
     */
    public RawTables tables() {
        return RawTables.of(getClient());
    }

    /**
     * Returns {@link RawRows} representing the Cognite Raw rows api endpoint.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.raw().rows();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     *
     * @return The raw rows api object.
     */
    public RawRows rows() {
        return RawRows.of(getClient());
    }


    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Raw build();
    }
}
