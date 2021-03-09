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

import com.cognite.client.Request;

import java.util.concurrent.CompletableFuture;

/**
 * An interface that defines the basic interface for calling a Cognite API endpoint with a request
 * and consuming the response.
 *
 * @param <T>
 */
public interface Connector<T> {
    /**
     * Executes a request against the api endpoint. This method will block until the request is completed and
     * the response can be consumed by the client.
     *
     * @param requestParameters
     * @return
     */
    ResponseItems<T> execute(Request requestParameters) throws Exception;

    /**
     * Executes a request against the api endpoint on a separate thread. The response is wrapped in a
     * future that is returned to the client immediately.
     *
     * @param requestParameters
     * @return
     */
    CompletableFuture<ResponseItems<T>> executeAsync(Request requestParameters) throws Exception;
}
