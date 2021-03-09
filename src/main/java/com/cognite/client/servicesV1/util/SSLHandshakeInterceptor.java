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

package com.cognite.client.servicesV1.util;

import okhttp3.CipherSuite;
import okhttp3.Handshake;
import okhttp3.Response;
import okhttp3.TlsVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Prints TLS Version and Cipher Suite for SSL Calls through OkHttp3 */
public class SSLHandshakeInterceptor implements okhttp3.Interceptor {
    private static final String TAG = "OkHttp3-SSLHandshake, ";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    public Response intercept(Chain chain) throws IOException {
        final Response response = chain.proceed(chain.request());
        printTlsAndCipherSuiteInfo(response);
        return response;
    }

    private void printTlsAndCipherSuiteInfo(Response response) {
        if (response != null) {
            Handshake handshake = response.handshake();
            if (handshake != null) {
                final CipherSuite cipherSuite = handshake.cipherSuite();
                final TlsVersion tlsVersion = handshake.tlsVersion();
                LOG.info(TAG + "TLS: " + tlsVersion + ", CipherSuite: " + cipherSuite);
            }
        }
    }
}
