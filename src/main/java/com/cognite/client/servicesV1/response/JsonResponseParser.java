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
import com.google.common.collect.ImmutableList;

import java.io.IOException;

/**
 * Parses the complete json payload as a single item.
 *
 */
@AutoValue
public abstract class JsonResponseParser extends DefaultResponseParser {

    static Builder builder() {
        return new AutoValue_JsonResponseParser.Builder();
    }

    public static JsonResponseParser create() {
        return JsonResponseParser.builder().build();
    }

    /**
     * Extract the main items from a results json payload. Returns the entire payload as a json string.
     *
     * @param json The results json payload
     * @return
     * @throws IOException
     */
    public ImmutableList<String> extractItems(String json) throws Exception {
        // Check if the input string is valid json. Will throw an exception if it cannot be parsed.
        objectMapper.readTree(json);

        return ImmutableList.of(json);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract JsonResponseParser build();
    }
}
