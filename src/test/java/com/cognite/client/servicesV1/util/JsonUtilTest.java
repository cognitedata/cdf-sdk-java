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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilTest {
    @Test
    void deserializeProtoStruct() {
        Struct struct = Struct.newBuilder()
                .putFields("FieldA", Value.newBuilder().setStringValue("StringValue").build())
                .putFields("FieldB", Value.newBuilder().setBoolValue(true).build())
                .putFields("FieldC", Value.newBuilder().setNumberValue(10d).build())
                .build();
        try {
            String directDeserialized = JsonFormat.printer().print(struct);
            String deserialized = JsonUtil.getObjectMapperInstance().writeValueAsString(struct);
            //System.out.printf("Direct: %s \n\r", directDeserialized);
            //System.out.printf("Via ObjectMapper: %s \n\r", deserialized);
            Assertions.assertEquals(deserialized, directDeserialized);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void deserializeCompositeObject() {
        Struct struct = Struct.newBuilder()
                .putFields("FieldA", Value.newBuilder().setStringValue("StringValue").build())
                .putFields("FieldB", Value.newBuilder().setBoolValue(true).build())
                .build();

        Map<String, Object> request = new HashMap<>();
        request.put("modelId", 1234);
        request.put("numMatches", 4);
        request.put("matchFrom", ImmutableList.of(struct));

        String expected = "{\"numMatches\":4,\"modelId\":1234,\"matchFrom\":[{\n" +
                "  \"FieldA\": \"StringValue\",\n" +
                "  \"FieldB\": true\n" +
                "}]}";

        try {
            String deserialized = JsonUtil.getObjectMapperInstance().writeValueAsString(request);
            //System.out.printf("Request: %s \n\r", deserialized);
            assertEquals(deserialized, expected);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}