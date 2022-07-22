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

package com.cognite.client.config;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.net.Proxy;

/**
 * Proxy server configuration.
 *
 * This class holds the proxy server configuration used by the {@link com.cognite.client.CogniteClient}. The default
 * configuration is no proxy.
 *
 * You can specify the proxy server to use via configuration of 1) the proxy server and (optionally) 2) the
 * username and password to use for authentication.
 *
 * <pre>
 * {@code
 * ProxyConfig proxyConfig = ProxyConfig.of(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)));
 * }
 * </pre>
 */
@AutoValue
public abstract class ProxyConfig {

  private static Builder builder() {
    return new AutoValue_ProxyConfig.Builder();
  }

  public static ProxyConfig of(Proxy proxy) {
    return ProxyConfig.builder()
            .setProxy(proxy)
            .build();
  }

  public abstract Proxy getProxy();
  @Nullable public abstract String getUsername();
  @Nullable public abstract String getPassword();

  public abstract ProxyConfig.Builder toBuilder();

  /**
   * Configures the proxy server authentication username.
   *
   * The default configuration is no authentication. I.e. no username / password.
   *
   * @param username The username for the proxy authentication.
   * @return the proxy configuration object with the username specified.
   */
  public ProxyConfig withUsername(String username) {
    return toBuilder().setUsername(username).build();
  }


  /**
   * Configures the proxy server authentication password.
   *
   * The default configuration is no authentication. I.e. no username / password.
   *
   * @param password The password for the proxy authentication.
   * @return the proxy configuration object with the username specified.
   */
  public ProxyConfig withPassword(String password) {
    return toBuilder().setPassword(password).build();
  }

  @Override
  public final String toString() {
    return "ProxyConfig{"
            + "proxy=" + getProxy() + ", "
            + "userName=" + getUsername() + ", "
            + "}";
  }

  @AutoValue.Builder public abstract static class Builder {
    abstract Builder setProxy(Proxy value);
    abstract Builder setUsername(String value);
    abstract Builder setPassword(String value);

    public abstract ProxyConfig build();
  }
}
