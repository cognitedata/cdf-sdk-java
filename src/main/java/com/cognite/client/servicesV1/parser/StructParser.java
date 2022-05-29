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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains a set of methods to help parsing {@link Struct} objects.
 */
public class StructParser {
    /**
     * Parses the main proto Struct (Json object equivalent) into a POJO representation.
     */
    public static Map<String, Object> parseStructToMap(Struct input) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, Value> entry : input.getFieldsMap().entrySet()) {
            map.put(entry.getKey(), StructParser.parseValueToObject(entry.getValue()));
        }
        return map;
    }

    /**
     * Parses the proto Value to POJO. This is needed to translate the proto Json representation into the Java object
     * representation used by the request parameters object.
     * @param input
     * @return
     */
    private static Object parseValueToObject(Value input) {
        if (input.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }
        if (input.getKindCase() == Value.KindCase.BOOL_VALUE) {
            return Boolean.valueOf(input.getBoolValue());
        }
        if (input.getKindCase() == Value.KindCase.STRING_VALUE) {
            return String.valueOf(input.getStringValue());
        }
        if (input.getKindCase() == Value.KindCase.NUMBER_VALUE) {
            return Double.valueOf(input.getNumberValue());
        }
        if (input.getKindCase() == Value.KindCase.LIST_VALUE) {
            ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
            for (Value listElement : input.getListValue().getValuesList()) {
                if (listElement.getKindCase() != Value.KindCase.NULL_VALUE) {
                    // Nulls are skipped.
                    listBuilder.add(StructParser.parseValueToObject(listElement));
                }
            }
            return listBuilder.build();
        }
        if (input.getKindCase() == Value.KindCase.STRUCT_VALUE) {
            return StructParser.parseStructToMap(input.getStructValue());
        }
        // If no compatible type is found (should not happen!) then just return null
        return null;
    }
}
