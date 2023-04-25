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
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Class representing the Cognite Data Fusion (CDF) project information:
 * - Host. The default host is {@code https://api.cognitedata.com}.
 * - Project. This is the name of a specific, isolated CDF environment.
 *
 * This information is used to uniquely identify which Cognite Data Fusion project/environment to connect with.
 */
@AutoValue
public abstract class AuthConfig implements Serializable {
  private final static String DEFAULT_HOST = "https://api.cognitedata.com";

  private static Builder builder() {
    return new AutoValue_AuthConfig.Builder()
            .setHost(DEFAULT_HOST);
  }

  public static AuthConfig of(String cdfProject) {
    Preconditions.checkArgument(null != cdfProject && !cdfProject.isBlank(),
            "The CDF Project cannot be null or blank.");

    return AuthConfig.builder()
            .setProject(cdfProject)
            .build();
  }

  @Nullable public abstract String getProject();
  public abstract String getHost();

  public abstract AuthConfig.Builder toBuilder();

  /**
   * Configure a specific Cognite Data Fusion host.
   *
   * This is relevant configuration for users of dedicated clusters or with custom routing configurations.
   *
   * The default host is {@code https://api.cognitedata.com}.
   *
   * @param host The CDF host to connect to.
   */
  public AuthConfig withHost(String host) {
    return toBuilder().setHost(host).build();
  }


  @Override
  public final String toString() {
    return "AuthConfig{"
            + "project=" + getProject() + ", "
            + "host=" + getHost() + ", "
            + "}";
  }

  @AutoValue.Builder public abstract static class Builder {
    abstract Builder setProject(String value);
    abstract Builder setHost(String value);

    public abstract AuthConfig build();
  }
}
