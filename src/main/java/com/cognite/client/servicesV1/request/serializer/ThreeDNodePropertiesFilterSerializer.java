package com.cognite.client.servicesV1.request.serializer;

import com.cognite.client.Request;
import com.cognite.client.dto.ThreeDNode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Class for Serializer object for pass different format filter ThreeDNode
 */
public class ThreeDNodePropertiesFilterSerializer extends StdSerializer<Request> {

    public ThreeDNodePropertiesFilterSerializer() {
        super(Request.class);
    }

    @Override
    public void serialize(Request request, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        if (request.getRequestParameters().containsKey("limit")) {
            gen.writeNumberField("limit", Integer.parseInt(request.getRequestParameters().get("limit").toString()));
        }
        if (request.getRequestParameters().containsKey("cursor")) {
            gen.writeStringField("cursor", request.getRequestParameters().get("cursor").toString());
        }
        if (request.getRequestParameters().containsKey("partition")) {
            gen.writeStringField("partition", request.getRequestParameters().get("partition").toString());
        }

        gen.writeObjectFieldStart("filter");
        gen.writeObjectFieldStart("properties");

        if (request.getRequestParameters().containsKey("filter")) {
            ImmutableMap<String, Object> map = (ImmutableMap<String, Object>) request.getRequestParameters().get("filter");
            ThreeDNode.PropertiesFilter filter =
                    (ThreeDNode.PropertiesFilter) map.get("properties");
            for (ThreeDNode.PropertiesFilter.Categories cat : filter.getCategoriesList()) {
                gen.writeObjectFieldStart(cat.getName());
                Map<String, ThreeDNode.PropertiesFilter.Categories.CategoriesValues> catValuesMap = cat.getValuesMap();
                Set<Map.Entry<String, ThreeDNode.PropertiesFilter.Categories.CategoriesValues>> set = catValuesMap.entrySet();
                for (Map.Entry<String, ThreeDNode.PropertiesFilter.Categories.CategoriesValues> entry : set) {
                    gen.writeArrayFieldStart(entry.getKey());

                    String values[] = new String[entry.getValue().getValuesStringCount()];
                    for (int i = 0; i < entry.getValue().getValuesStringCount(); i++) {
                        gen.writeObject(entry.getValue().getValuesString(i));
                    }
                    gen.writeEndArray();
                }
                gen.writeEndObject();
            }
        }
        gen.writeEndObject();
    }
}
