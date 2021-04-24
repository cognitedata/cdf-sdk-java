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
import java.io.Serializable;

@AutoValue
public abstract class AuthConfig implements Serializable {
  private final static String DEFAULT_HOST = "https://api.cognitedata.com";

  private static Builder builder() {
    return new AutoValue_AuthConfig.Builder()
            .setHost(DEFAULT_HOST);
  }

  public static AuthConfig create() {
    return AuthConfig.builder().build();
  }

  @Nullable public abstract String getProject();
  public abstract String getHost();

  public abstract AuthConfig.Builder toBuilder();

  /**
   * Returns a new {@link AuthConfig} that represents the specified host.
   * @param value The project id interact with.
   */
  public AuthConfig withHost(String value) {
    return toBuilder().setHost(value).build();
  }


  /**
   * Returns a new {@link AuthConfig} that represents the specified project.
   * @param value The project id interact with.
   */
  public AuthConfig withProject(String value) {
    return toBuilder().setProject(value).build();
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
