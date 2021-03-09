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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public final class CharSplitter {
  private static final String regex = "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
  private static final List<Character> specialCharacters = ImmutableList.of('.', '*');

  private String wrap(Character delimiter) {
    if (specialCharacters.contains(delimiter)) {
      return "\\" + delimiter;
    }

    return String.valueOf(delimiter);
  }

  @SuppressWarnings("unchecked")
  public Collection<String> split(Character delimiter, String string) {
    if (Strings.isNullOrEmpty(string)) {
      return Collections.EMPTY_LIST;
    }

    return Arrays.stream(string.split(wrap(delimiter) + regex, -1))
        .map(s -> {
          if (s.length() > 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            return s.substring(1, s.length() - 1);
          }
          return s;
        })
        .collect(Collectors.toList());
  }
}
