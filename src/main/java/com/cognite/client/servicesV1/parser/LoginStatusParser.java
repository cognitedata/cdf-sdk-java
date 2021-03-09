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

import com.cognite.client.dto.LoginStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing item object between Cognite api representations
 * (json and proto) and typed objects.
 */
public class LoginStatusParser {
    static final Logger LOG = LoggerFactory.getLogger(LoginStatusParser.class);
    static final String logPrefix = "LoginStatusParser - ";
    static final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a <code>List</code> of json items (payload from the Cognite api) into a <code>List</code>
     * of <code>LoginStatus</code>.
     *
     * @param input
     * @return
     * @throws Exception
     */
    public static List<LoginStatus> parseLoginStatuses(List<String> input) throws Exception {
        ImmutableList.Builder<LoginStatus> listBuilder = ImmutableList.builder();
        for (String item : input) {
            listBuilder.add(LoginStatusParser.parseLoginStatus(item));
        }
        return listBuilder.build();
    }

    /**
     * Parses a <code>String</code> (json payload from the Cognite api) into a <code>LoginStatus</code>
     *
     * @return
     */
    public static LoginStatus parseLoginStatus(String loginStatusJson) throws Exception {
        LOG.trace(logPrefix + "Start parsing login status from Json to DTO.");
        LOG.trace(logPrefix + "Json input: \r\n {}", loginStatusJson);
        try {
            JsonNode root = objectMapper.readTree(loginStatusJson).get("data");
            LoginStatus.Builder loginStatusBuilder = LoginStatus.newBuilder();

            // Mandatory fields
            if (root.path("user").isTextual()) {
                loginStatusBuilder.setUser(root.get("user").textValue());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: user. Item excerpt: "
                        + LoginStatusParser.getJsonExcerpt(loginStatusJson);

                LOG.error(message);
                throw new Exception(message);
            }

            if (root.path("loggedIn").isBoolean()) {
                loginStatusBuilder.setLoggedIn(root.get("loggedIn").booleanValue());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: loggedIn. Item excerpt: "
                        + LoginStatusParser.getJsonExcerpt(loginStatusJson);

                LOG.error(message);
                throw new Exception(message);
            }

            if (root.path("project").isTextual()) {
                loginStatusBuilder.setProject(root.get("project").textValue());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: project. Item excerpt: "
                        + LoginStatusParser.getJsonExcerpt(loginStatusJson);

                LOG.error(message);
                throw new Exception(message);
            }

            if (root.path("projectId").isIntegralNumber()) {
                loginStatusBuilder.setProjectId(root.get("projectId").longValue());
            } else {
                String message = logPrefix + parseErrorDefaultPrefix
                        + "Unable to parse attribute: projectId. Item excerpt: "
                        + LoginStatusParser.getJsonExcerpt(loginStatusJson);

                LOG.error(message);
                throw new Exception(message);
            }

            // optional fields
            if (root.path("apiKeyId").isIntegralNumber()) {
                loginStatusBuilder.setApiKeyId(Int64Value.of(root.get("apiKeyId").longValue()));
            }

            LOG.trace(logPrefix + "Item built: {}", loginStatusBuilder.toString());
            return loginStatusBuilder.build();
        } catch (Exception e) {
            LOG.error(logPrefix + parseErrorDefaultPrefix, e);
            throw e;
        }
    }

    private static String getJsonExcerpt(String json) {
        return json.substring(0, Math.min(json.length(), MAX_LOG_ELEMENT_LENGTH));
    }
}
