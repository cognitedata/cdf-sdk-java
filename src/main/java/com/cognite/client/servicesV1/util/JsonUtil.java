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


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;

/**
 * Utilities class that provides central configurations of JSON handling.
 *
 * It offers a Jackson {@link ObjectMapper} instance that is pre-configured to handle serialization of
 * Protobuf {@link Message}.
 */
public class JsonUtil {
    private static ObjectMapper objectMapper;

    private JsonUtil() {}

    /**
     * Returns a pre-configured {@link ObjectMapper} singleton. You should use this as the starting point
     * for instantiating readers and writers.
     *
     * @return A pre-configured singleton.
     */
    public static ObjectMapper getObjectMapperInstance() {
        if (null == objectMapper) {
            objectMapper = new ObjectMapper();
            SimpleModule module = new SimpleModule("ProtoSerializer");
            module.addSerializer(Message.class, new ProtoSerializer());
            objectMapper.registerModule(module);
        }

        return objectMapper;
    }


    private static class ProtoSerializer extends StdSerializer<Message> {

        public ProtoSerializer() {
            super(Message.class);
        }

        @Override
        public void serialize(Message message,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeRawValue(JsonFormat.printer().print(message));
        }
    }
}
