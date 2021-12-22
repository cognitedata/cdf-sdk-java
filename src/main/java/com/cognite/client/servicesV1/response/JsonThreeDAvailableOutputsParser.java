package com.cognite.client.servicesV1.response;

import com.cognite.client.dto.ThreeDAvailableOutput;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;

@AutoValue
public abstract class JsonThreeDAvailableOutputsParser extends DefaultResponseParser {

    public static JsonThreeDAvailableOutputsParser.Builder builder() {
        return new AutoValue_JsonThreeDAvailableOutputsParser.Builder();
    }

    public abstract Builder toBuilder();

    @Override
    public ImmutableList<String> extractItems(String json) throws Exception {
        ArrayList<String> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("items");
        if (node.isArray()) {
            LOG.debug("Found items array in json response payload.");
            for (JsonNode child : node) {
                if (child.isObject()) {
                    tempList.add(child.toString());
                } else {
                    tempList.add(child.textValue());
                }
            }
            return ImmutableList.copyOf(tempList);
        }
        LOG.info("items array not found in Json payload: \r\n" + json
                .substring(0, Math.min(MAX_LENGTH_JSON_LOG, json.length())));
        return ImmutableList.of();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract JsonThreeDAvailableOutputsParser build();
    }
}
