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

package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;

import java.util.Optional;

public interface RequestProvider {

  /**
   * Set the baseline <code>Request</code> to build requests from. This is typically the application-provided
   * <code>Request</code> that kicks off a potential series of requests (in the case of iterating
   * over a results set).
   *
   * This baseline <code>Request</code> do not change over the life-cycle of a request-response series.
   *
   * @param requestParameters
   * @return
   */
  RequestProvider withRequest(Request requestParameters);

  /**
   * Builds a request based on the basline <code>RequestParameters</code> provided via the <code>withRequestParameters</code>
   * method.
   *
   * If a cursor is provided, this will typically lead the <code>RequestProvider</code> to modify the underlying
   * <code>Request</code>. In order to get the exact <code>Request</code> used to build the latest
   * request, call the <code>getEffectiveRequestParameters</code> method.
   *
   * @param cursor
   * @return
   * @throws Exception
   */
  okhttp3.Request buildRequest(Optional<String> cursor) throws Exception;
}
