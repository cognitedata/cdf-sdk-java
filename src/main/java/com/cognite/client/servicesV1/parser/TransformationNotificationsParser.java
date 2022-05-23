package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.Transformation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class TransformationNotificationsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Builds an object {@link Transformation.Notification} from json value.
     *
     * @param json
     * @return
     * @throws JsonProcessingException
     */
    public static Transformation.Notification parseTransformationNotifications(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        Transformation.Notification.Builder tmBuilder = Transformation.Notification.newBuilder();

        if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    private static void extractNodes(Transformation.Notification.Builder tmBuilder, JsonNode root) {
        if (root.path("id").isNumber()) {
            tmBuilder.setId(root.get("id").longValue());
        }
        if (root.path("transformationId").isNumber()) {
            tmBuilder.setTransformationId(root.get("transformationId").longValue());
        }
        if (root.path("createdTime").isLong()) {
            tmBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isLong()) {
            tmBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }
        if (root.path("destination").isTextual()) {
            tmBuilder.setDestination(root.get("destination").textValue());
        }
    }

    /**
     * Builds a request insert item object from {@link Transformation.Notification.Subscription}.
     *
     * An insert item object creates a new Transformation.Notification.Subscribe object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(Transformation.Notification.Subscription element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasTransformationId() || element.hasTransformationExternalId(),
                "Element must have [transformationId or transformationExternalId] in order to be written");
        Preconditions.checkNotNull(element.getDestination(),
                "Unable to find attribute [destination] in the Subscribe object. ");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        mapBuilder.put("destination", element.getDestination());

        if (element.hasTransformationId() && element.hasTransformationExternalId()) {
            throw new Exception("Only one of the fields [transformationId or transformationExternalId] must be filled");
        }else if (element.hasTransformationId()) {
            mapBuilder.put("transformationId", element.getTransformationId());
        } else if (element.hasTransformationId()) {
            mapBuilder.put("transformationExternalId", element.getTransformationExternalId());
        } else {
            throw new Exception("Unable to find attribute [transformationId or transformationExternalId] in the Subscribe object. ");
        }

        return mapBuilder.build();
    }

    /**
     * Builds a List of {@link Transformation.Notification} from json value.
     *
     * @param json
     * @return
     * @throws JsonProcessingException
     */
    public static List<Transformation.Notification> parseTransformationNotificationsToList(String json) throws JsonProcessingException {
        List<Transformation.Notification> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        Transformation.Notification.Builder tmBuilder = Transformation.Notification.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
                list.add(tmBuilder.build());
                tmBuilder.clear();
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
            list.add(tmBuilder.build());
        }

        return list;
    }
}
