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

import com.cognite.client.dto.FileBinary;
import com.cognite.client.Request;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Parses a file binary based on a request and binary response payload.
 *
 * The request parameters must contain a single item with either the *externalId* or the *id*. The response payload
 * represents the file binary.
 */
@AutoValue
public abstract class FileBinaryResponseParser implements ResponseParser<FileBinary> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    // Logger identifier per instance
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);

    static FileBinaryResponseParser.Builder builder() {
        return new AutoValue_FileBinaryResponseParser.Builder();
    }

    public static FileBinaryResponseParser create() {
        return FileBinaryResponseParser.builder()
                .setRequest(Request.create()
                        .withItemExternalIds("defaultBeamId"))
                .build();
    }

    public static FileBinaryResponseParser of(Request requestParameters) {
        return FileBinaryResponseParser.builder()
                .setRequest(requestParameters)
                .build();
    }

    public abstract FileBinaryResponseParser.Builder toBuilder();
    abstract Request getRequest();

    public FileBinaryResponseParser withRequest(Request requestParameters) {
        return toBuilder().setRequest(requestParameters).build();
    }

    /**
     * Returns <code>Optional.empty()</code> as there is no next cursor in a file binary response.
     *
     * @param payload The response body
     * @return
     * @throws Exception
     */
    public Optional<String> extractNextCursor(byte[] payload) throws Exception {
        // There is never a next cursor in a file binary payload
        return Optional.empty();
    }

    /**
     * Extract the file binary from the response body.
     *
     * @param payload The reponse body
     * @return
     * @throws Exception
     */
    public ImmutableList<FileBinary> extractItems(byte[] payload) throws Exception {
        final String loggingPrefix = "Extract FileBinary items from payload [" + randomIdString + "] -";
        LOG.info(loggingPrefix + "Start extracting file from payload.");
        FileBinary.Builder fileBinaryBuilder = FileBinary.newBuilder();

        // Get the id from the request
        Map<String, Object> item = getRequest().getItems().get(0);
        Preconditions.checkState(item != null && (item.containsKey("id") || item.containsKey("externalId")),
                "Request parameters does not contain a valid item.");
        if (item.containsKey("externalId")) {
            fileBinaryBuilder.setExternalId((String) item.get("externalId"));
        } else {
            fileBinaryBuilder.setId((Long) item.get("id"));
        }

        // Get the binary
        fileBinaryBuilder.setBinary(ByteString.copyFrom(payload));

        return ImmutableList.of(fileBinaryBuilder.build());
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract FileBinaryResponseParser.Builder setRequest(Request value);

        abstract FileBinaryResponseParser autoBuild();

        public FileBinaryResponseParser build() {
            FileBinaryResponseParser parser = autoBuild();

            Preconditions.checkState(parser.getRequest().getItems().size() == 1,
                    "The request parameters must contain a single item");

            return parser;
        }
    }
}
