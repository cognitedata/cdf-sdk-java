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

import java.util.Optional;

@AutoValue
public abstract class FileUploadHeaderResponseParser extends DefaultResponseParser {

    public static Builder builder() {
        return new AutoValue_FileUploadHeaderResponseParser.Builder();
    }

    public abstract Builder toBuilder();

    public Optional<String> extractNextCursor(String json) throws Exception {
        LOG.info("No cursor in file upload response");
        return Optional.empty();
    }

    public ImmutableList<String> extractItems(String json) throws Exception {
        // the response from file upload is a single item Json--just return it as-is.
        return ImmutableList.of(json);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract FileUploadHeaderResponseParser build();
    }
}
