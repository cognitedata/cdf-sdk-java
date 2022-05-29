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

import com.cognite.client.dto.RawRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;

import java.util.HashMap;
import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class RawParser {
    static final String logPrefix = "RawParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an raw row json string to {@link RawRow} proto object.
     *
     * @return
     * @throws Exception
     */
    public static RawRow parseRawRow(String dbName, String dbTable, String rowJson) throws Exception {
        Preconditions.checkNotNull(dbName, "dbName cannot be null");
        Preconditions.checkNotNull(dbTable, "dbTable cannot be null");
        Preconditions.checkNotNull(rowJson, "rowJson cannot be null");
        String logItemExcerpt = rowJson.substring(0, Math.min(rowJson.length() - 1, MAX_LOG_ELEMENT_LENGTH));

        JsonNode root = objectMapper.readTree(rowJson);
        RawRow.Builder rowBuilder = RawRow.newBuilder()
                .setDbName(dbName)
                .setTableName(dbTable);

        // A raw row must contain key, last updated time and columns.
        if (root.path("key").isTextual()) {
            rowBuilder.setKey(root.get("key").textValue());
        } else {
            throw new Exception(logPrefix
                    + "Unable to parse attribute: key. Item exerpt: "
                    + logItemExcerpt);
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            rowBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        } else {
            throw new Exception(logPrefix
                    + "Unable to parse attribute: lastUpdatedTime. Item exerpt: "
                    + logItemExcerpt);
        }
        if (root.path("columns").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(objectMapper.writeValueAsString(root.get("columns")), structBuilder);
            rowBuilder.setColumns(structBuilder.build());
        } else {
            throw new Exception(logPrefix
                    + "Unable to parse attribute: columns. Item exerpt: "
                    + logItemExcerpt);
        }

       return rowBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>RawRow</code>.
     *
     * An insert item object creates a new raw row data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(RawRow element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        mapBuilder.put("key", element.getKey());
        if (element.hasColumns()) {
            mapBuilder.put("columns", StructParser.parseStructToMap(element.getColumns()));
        }
        return mapBuilder.build();
    }
}
