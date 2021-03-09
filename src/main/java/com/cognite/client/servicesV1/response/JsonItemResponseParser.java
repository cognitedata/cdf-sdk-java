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

package com.cognite.client.servicesV1.response;

import com.google.auto.value.AutoValue;

/**
 * Extracts results items from the {@code items} node and
 * next cursor from the {@code nextCursor} node.
 */
@AutoValue
public abstract class JsonItemResponseParser extends DefaultResponseParser {

    static Builder builder() {
        return new com.cognite.client.servicesV1.response.AutoValue_JsonItemResponseParser.Builder();
    }

    public static JsonItemResponseParser create() {
        return JsonItemResponseParser.builder().build();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract JsonItemResponseParser build();
    }
}
