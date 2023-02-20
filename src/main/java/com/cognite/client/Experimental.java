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

/**
 * This class represents api services in "playground". That is, api services
 * that are in an early development phase. The signature and behavior
 * of these services may change without notification.
 *
 */
@AutoValue
public abstract class Experimental extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Experimental.Builder();
    }

    /**
     * Constructs a new {@link Experimental} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Experimental of(CogniteClient client) {
        return Experimental.builder()
                .setClient(client)
                .build();
    }


    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Experimental build();
    }
}
