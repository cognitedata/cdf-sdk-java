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

package com.cognite.client.servicesV1;

import com.cognite.client.servicesV1.response.JsonErrorItemResponseParser;
import com.cognite.client.servicesV1.response.JsonStringAttributeResponseParser;
import com.cognite.client.servicesV1.response.ResponseParser;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This class represents a collection of response items from a Cognite API request.
 *
 *
 */
@AutoValue
public abstract class ResponseItems<T> implements Serializable {
    static <T> Builder<T> builder() {
        return new com.cognite.client.servicesV1.AutoValue_ResponseItems.Builder<T>()
                .setDuplicateResponseParser(JsonErrorItemResponseParser.builder()
                        .setErrorSubPath("duplicated")
                        .build())
                .setMissingResponseParser(JsonErrorItemResponseParser.builder()
                        .setErrorSubPath("missing")
                        .build())
                .setErrorMessageResponseParser(JsonStringAttributeResponseParser.create()
                        .withAttributePath("error.message"))
                .setStatusResponseParser(JsonStringAttributeResponseParser.create()
                        .withAttributePath("error.code"));
    }

    public static <T> ResponseItems<T> of(ResponseParser<T> responseParser, ResponseBinary response) {
        Preconditions.checkNotNull(responseParser, "Response parser cannot be null.");
        Preconditions.checkNotNull(response, "Response binary cannot be null.");

        return ResponseItems.<T>builder()
                .setResponseParser(responseParser)
                .setResponseBinary(response)
                .build();
    }

    abstract Builder<T> toBuilder();

    abstract ResponseParser<T> getResponseParser();
    abstract ResponseParser<String> getDuplicateResponseParser();
    abstract ResponseParser<String> getMissingResponseParser();
    abstract ResponseParser<String> getErrorMessageResponseParser();
    abstract ResponseParser<String> getStatusResponseParser();
    @Nullable
    abstract ImmutableList<T> getResultsItemsList();
    public abstract ResponseBinary getResponseBinary();

    /**
     * Specifies the parser for extracting duplicate items from the response payload.
     *
     * The default parser looks in the *duplicated* object of the response json.
     * @param parser
     * @return
     */
    public ResponseItems<T> withDuplicateResponseParser(ResponseParser<String> parser) {
        Preconditions.checkNotNull(parser, "Parser cannot be null.");
        return toBuilder().setDuplicateResponseParser(parser).build();
    }

    /**
     * Specifies the parser for extracting missing items from the response payload.
     *
     * The default parser looks in the *missing* object of the response json.
     * @param parser
     * @return
     */
    public ResponseItems<T> withMissingResponseParser(ResponseParser<String> parser) {
        Preconditions.checkNotNull(parser, "Parser cannot be null.");
        return toBuilder().setMissingResponseParser(parser).build();
    }

    /**
     * Specifies the parser for extracting the error message from the response payload.
     *
     * The default parser looks in the *message* attribute of the response json.
     * @param parser
     * @return
     */
    public ResponseItems<T> withErrorMessageResponseParser(ResponseParser<String> parser) {
        Preconditions.checkNotNull(parser, "Parser cannot be null.");
        return toBuilder().setErrorMessageResponseParser(parser).build();
    }

    /**
     * Specifies the parser for extracting the error code (http status code) from the response payload.
     *
     * The default parser looks in the *code* attribute of the response json.
     * @param parser
     * @return
     */
    public ResponseItems<T> withStatusResponseParser(ResponseParser<String> parser) {
        Preconditions.checkNotNull(parser, "Parser cannot be null.");
        return toBuilder().setStatusResponseParser(parser).build();
    }

    /**
     * Specifies a results items list. By setting this list you will override the default behavior
     * which is to parse the results items from the response body (bytes).
     *
     * That is, if a results items list is specified, the parser will not attempt to parse the response body.
     *
     * @param itemsList
     * @return
     */
    public ResponseItems<T> withResultsItemsList(ImmutableList<T> itemsList) {
        return toBuilder().setResultsItemsList(itemsList).build();
    }

    /**
     * Returns the main result items.
     *
     * @return A list of result items.
     * @throws Exception
     */
    public ImmutableList<T> getResultsItems() throws Exception {
        if (null != getResultsItemsList()) {
            // a results items list is specified, so this will override the parser
            return getResultsItemsList();
        }
        return getResponseParser().extractItems(getResponseBinary().getResponseBodyBytes().toByteArray());
    }

    /**
     * Returns a list of duplicate items.
     *
     *
     * @return A list of duplicate items.
     * @throws Exception
     */
    public ImmutableList<String> getDuplicateItems() throws Exception {
        return getDuplicateResponseParser().extractItems(getResponseBinary().getResponseBodyBytes().toByteArray());
    }

    public ImmutableList<String> getMissingItems() throws Exception {
        return getMissingResponseParser().extractItems(getResponseBinary().getResponseBodyBytes().toByteArray());
    }

    public ImmutableList<String> getStatus() throws Exception {
        return getStatusResponseParser().extractItems(getResponseBinary().getResponseBodyBytes().toByteArray());
    }

    public ImmutableList<String> getErrorMessage() throws Exception {
        return getErrorMessageResponseParser().extractItems(getResponseBinary().getResponseBodyBytes().toByteArray());
    }

    public String getResponseBodyAsString() {
        return getResponseBinary().getResponseBodyBytes().toStringUtf8();
    }

    public boolean isSuccessful() {
        return getResponseBinary().getResponse().isSuccessful();
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
        abstract Builder<T> setResponseParser(ResponseParser<T> value);
        abstract Builder<T> setDuplicateResponseParser(ResponseParser<String> value);
        abstract Builder<T> setMissingResponseParser(ResponseParser<String> value);
        abstract Builder<T> setErrorMessageResponseParser(ResponseParser<String> value);
        abstract Builder<T> setStatusResponseParser(ResponseParser<String> value);
        abstract Builder<T> setResponseBinary(ResponseBinary value);
        abstract Builder<T> setResultsItemsList(ImmutableList<T> value);

        abstract ResponseItems<T> build();
    }
}
