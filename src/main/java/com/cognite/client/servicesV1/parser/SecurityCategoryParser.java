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

package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.SecurityCategory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;

import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing security category objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class SecurityCategoryParser {
    static final String logPrefix = "LabelParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    public static SecurityCategory parseSecurityCategory(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        SecurityCategory.Builder securityCategoryBuilder = SecurityCategory.newBuilder();

        // A security category must contain an id and name.
        if (root.path("id").isIntegralNumber()) {
            securityCategoryBuilder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: id. Item excerpt: "
                    + json
                    .substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
        }
        if (root.path("name").isTextual()) {
            securityCategoryBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: name");
        }

        return securityCategoryBuilder.build();
    }

    public static Map<String, Object> toRequestInsertItem(SecurityCategory element) {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("name", element.getName());

        return mapBuilder.build();
    }
}
