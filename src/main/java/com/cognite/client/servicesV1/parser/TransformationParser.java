package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Transformation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class TransformationParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Builds a request insert item object from {@link Transformation}.
     *
     * An insert item object creates a new Transformation object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(Transformation element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (StringUtils.isNotBlank(element.getName())) {
            mapBuilder.put("name", element.getName());
        } else {
            throw new Exception("Unable to find attribute [name] in the Transformation object. ");
        }

        if (StringUtils.isNotBlank(element.getExternalId())) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            throw new Exception("Unable to find attribute [externalId] in the Transformation object. ");
        }

        if (Boolean.valueOf(element.getIgnoreNullFields()) != null) {
            mapBuilder.put("ignoreNullFields", element.getIgnoreNullFields());
        } else {
            throw new Exception("Unable to find attribute [ignoreNullFields] in the Transformation object. ");
        }

        // Optional fields
        if (element.hasQuery()) {
            mapBuilder.put("query", element.getQuery());
        }

        if (element.hasDestination()) {
            Transformation.Destination.Builder builder = Transformation.Destination.newBuilder(element.getDestination());
            builder.setType(element.getDestination().getType().toLowerCase());
            mapBuilder.put("destination", builder.build());
        }

        if (element.hasConflictMode()) {
            mapBuilder.put("conflictMode", element.getConflictMode());
        }

        if (element.hasIsPublic()) {
            mapBuilder.put("isPublic", element.getIsPublic());
        }

        if (element.hasSourceApiKey() && StringUtils.isNotBlank(element.getSourceApiKey())) {
            mapBuilder.put("sourceApiKey", element.getSourceApiKey());
        }

        if (element.hasDestinationApiKey() && StringUtils.isNotBlank(element.getDestinationApiKey())) {
            mapBuilder.put("destinationApiKey", element.getDestinationApiKey());
        }

        if (element.hasSourceOidcCredentials() && isValidOidcCredentials(element.getSourceOidcCredentials())) {
            mapBuilder.put("sourceOidcCredentials", element.getSourceOidcCredentials());
        }

        if (element.hasDestinationOidcCredentials() && isValidOidcCredentials(element.getDestinationOidcCredentials())) {
            mapBuilder.put("destinationOidcCredentials", element.getDestinationOidcCredentials());
        }

        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
        }

        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestUpdateItem(Transformation element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(StringUtils.isNotBlank(element.getExternalId()) || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        if (StringUtils.isNotBlank(element.getExternalId())) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (StringUtils.isNotBlank(element.getName())) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));
        }

        if (element.hasQuery()) {
            updateNodeBuilder.put("query", ImmutableMap.of("set", element.getQuery()));
        }

        if (element.hasDestination()) {
            updateNodeBuilder.put("destination", ImmutableMap.of("set", element.getDestination()));
        }

        if (element.hasConflictMode()) {
            updateNodeBuilder.put("conflictMode", ImmutableMap.of("set", element.getConflictMode()));
        }

        if (element.hasIsPublic()) {
            updateNodeBuilder.put("isPublic", ImmutableMap.of("set", element.getIsPublic()));
        }

        if (element.hasSourceApiKey() && StringUtils.isNotBlank(element.getSourceApiKey())) {
            updateNodeBuilder.put("sourceApiKey", ImmutableMap.of("set", element.getSourceApiKey()));
        }

        if (element.hasDestinationApiKey() && StringUtils.isNotBlank(element.getDestinationApiKey())) {
            updateNodeBuilder.put("destinationApiKey", ImmutableMap.of("set", element.getDestinationApiKey()));
        }

        if (element.hasSourceOidcCredentials() && isValidOidcCredentials(element.getSourceOidcCredentials())) {
            updateNodeBuilder.put("sourceOidcCredentials", ImmutableMap.of("set", element.getSourceOidcCredentials()));
        }

        if (element.hasDestinationOidcCredentials() && isValidOidcCredentials(element.getDestinationOidcCredentials())) {
            updateNodeBuilder.put("destinationOidcCredentials", ImmutableMap.of("set", element.getDestinationOidcCredentials()));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestReplaceItem(Transformation element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(StringUtils.isNotBlank(element.getExternalId()) || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        if (StringUtils.isNotBlank(element.getExternalId())) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (StringUtils.isNotBlank(element.getName())) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));
        }

        if (StringUtils.isNotBlank(element.getName())) {
            updateNodeBuilder.put("query", ImmutableMap.of("set", element.getName()));
        } else {
            updateNodeBuilder.put("query", ImmutableMap.of("setNull", true));
        }

        if (element.hasDestination()) {
            updateNodeBuilder.put("destination", ImmutableMap.of("set", element.getDestination()));
        } else {
            updateNodeBuilder.put("destination", ImmutableMap.of("setNull", true));
        }


        if (element.hasConflictMode()) {
            updateNodeBuilder.put("conflictMode", ImmutableMap.of("set", element.getConflictMode()));
        } else {
            updateNodeBuilder.put("conflictMode", ImmutableMap.of("setNull", true));
        }

        if (element.hasIsPublic()) {
            updateNodeBuilder.put("isPublic", ImmutableMap.of("set", element.getIsPublic()));
        } else {
            updateNodeBuilder.put("isPublic", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceApiKey()) {
            if (StringUtils.isNotBlank(element.getSourceApiKey())) {
                updateNodeBuilder.put("sourceApiKey", ImmutableMap.of("set", element.getSourceApiKey()));
            }
        } else {
            updateNodeBuilder.put("sourceApiKey", ImmutableMap.of("setNull", true));
        }

        if (element.hasDestinationApiKey()) {
            if (StringUtils.isNotBlank(element.getDestinationApiKey())) {
                updateNodeBuilder.put("destinationApiKey", ImmutableMap.of("set", element.getDestinationApiKey()));
            }
        } else {
            updateNodeBuilder.put("destinationApiKey", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceOidcCredentials()) {
            if (isValidOidcCredentials(element.getSourceOidcCredentials())) {
                updateNodeBuilder.put("sourceOidcCredentials", ImmutableMap.of("set", element.getSourceOidcCredentials()));
            }
        } else {
            updateNodeBuilder.put("sourceOidcCredentials", ImmutableMap.of("setNull", true));
        }

        if (element.hasDestinationOidcCredentials()) {
            if (isValidOidcCredentials(element.getDestinationOidcCredentials())) {
                updateNodeBuilder.put("destinationOidcCredentials", ImmutableMap.of("set", element.getDestinationOidcCredentials()));
            }
        } else {
            updateNodeBuilder.put("destinationOidcCredentials", ImmutableMap.of("setNull", true));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    public static Transformation parseTransformations(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        Transformation.Builder tmBuilder = Transformation.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    public static List<Transformation> parseTransformationsToList(String json) throws JsonProcessingException {
        List<Transformation> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        Transformation.Builder tmBuilder = Transformation.newBuilder();
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

    private static void extractNodes(Transformation.Builder tmBuilder, JsonNode node) {
        if (node.path("id").isIntegralNumber()) {
            tmBuilder.setId(node.get("id").longValue());
        }
        if (node.path("name").isTextual()) {
            tmBuilder.setName(node.get("name").textValue());
        }
        if (node.path("query").isTextual()) {
            tmBuilder.setQuery(node.get("query").textValue());
        }
        if (node.path("destination").isObject()) {
            JsonNode destNode = node.path("destination");
            Transformation.Destination.Builder builderDest = Transformation.Destination.newBuilder();
            if (destNode.path("type").isTextual()) {
                builderDest.setType(destNode.path("type").textValue());
            }
            if (destNode.path("database").isTextual()) {
                builderDest.setDatabase(destNode.path("database").textValue());
            }
            if (destNode.path("table").isTextual()) {
                builderDest.setTable(destNode.path("table").textValue());
            }
            if (destNode.path("externalId").isTextual()) {
                builderDest.setExternalId(destNode.path("externalId").textValue());
            }
        }
        if (node.path("conflictMode").isTextual()) {
            tmBuilder.setConflictMode(node.get("conflictMode").textValue());
        }
        if (node.path("isPublic").isBoolean()) {
            tmBuilder.setIsPublic(node.get("isPublic").booleanValue());
        }
        if (node.path("sourceApiKey").isTextual()) {
            tmBuilder.setSourceApiKey(node.get("sourceApiKey").textValue());
        }
        if (node.path("destinationApiKey").isTextual()) {
            tmBuilder.setDestinationApiKey(node.get("destinationApiKey").textValue());
        }
        if (node.path("externalId").isTextual()) {
            tmBuilder.setExternalId(node.get("externalId").textValue());
        }
        if (node.path("ignoreNullFields").isBoolean()) {
            tmBuilder.setIgnoreNullFields(node.get("ignoreNullFields").booleanValue());
        }
        if (node.path("dataSetId").isNumber()) {
            tmBuilder.setDataSetId(node.get("dataSetId").longValue());
        }
        if (node.path("createdTime").isNumber()) {
            tmBuilder.setCreatedTime(node.get("createdTime").longValue());
        }
        if (node.path("lastUpdatedTime").isNumber()) {
            tmBuilder.setLastUpdatedTime(node.get("lastUpdatedTime").longValue());
        }
        if (node.path("hasSourceApiKey").isBoolean()) {
            tmBuilder.setSourceApiKey("");
        }
        if (node.path("hasDestinationApiKey").isBoolean()) {
            tmBuilder.setDestinationApiKey("");
        }
        if (node.path("hasSourceOidcCredentials").isBoolean()) {
            tmBuilder.setSourceOidcCredentials(Transformation.FlatOidcCredentials.newBuilder()
                    .setClientId("")
                    .setClientSecret("")
                    .setCdfProjectName("")
                    .setTokenUri("")
                    .setAudience("")
                    .setScopes("")
                    .build());
        }
        if (node.path("hasDestinationOidcCredentials").isBoolean()) {
            tmBuilder.setDestinationOidcCredentials(Transformation.FlatOidcCredentials.newBuilder()
                    .setClientId("")
                    .setClientSecret("")
                    .setCdfProjectName("")
                    .setTokenUri("")
                    .setAudience("")
                    .setScopes("")
                    .build());
        }

        if (node.path("owner").isObject()) {
            JsonNode ownerNode = node.path("owner");
            Transformation.Owner.Builder ownerBuilder = Transformation.Owner.newBuilder();
            if (ownerNode.path("user").isTextual()) {
                ownerBuilder.setUser(ownerNode.get("user").textValue());
            }
            tmBuilder.setOwner(ownerBuilder.build());
        }

        if (node.path("blocked").isObject()) {
            JsonNode blockedNode = node.path("blocked");
            Transformation.TransformBlockedInfo.Builder blockedBuilder = Transformation.TransformBlockedInfo.newBuilder();
            if (blockedNode.path("reason").isTextual()) {
                blockedBuilder.setReason(blockedNode.get("reason").textValue());
            }
            if (blockedNode.path("createdTime").isLong()) {
                blockedBuilder.setCreatedTime(blockedNode.get("createdTime").longValue());
            }
            tmBuilder.setBlocked(blockedBuilder.build());
        }

        if (node.path("lastFinishedJob").isObject()) {
            JsonNode nodeLastFinishedJob = node.path("lastFinishedJob");
            Transformation.Job.Builder lastJobBuilder = Transformation.Job.newBuilder();
            TransformationJobsParser.extractNodes(lastJobBuilder, nodeLastFinishedJob);
            tmBuilder.setLastFinishedJob(lastJobBuilder.build());
        }

        if (node.path("schedule").isObject()) {
            Transformation.Schedule.Builder scheduleBuilder = Transformation.Schedule.newBuilder();
            JsonNode scheduleNode = node.path("schedule");
            if (scheduleNode.path("id").isNumber()) {
                scheduleBuilder.setId(scheduleNode.get("id").intValue());
            }
            if (scheduleNode.path("externalId").isTextual()) {
                scheduleBuilder.setExternalId(scheduleNode.get("externalId").textValue());
            }
            if (scheduleNode.path("createdTime").isNumber()) {
                scheduleBuilder.setCreatedTime(scheduleNode.get("createdTime").longValue());
            }
            if (scheduleNode.path("lastUpdatedTime").isNumber()) {
                scheduleBuilder.setLastUpdatedTime(scheduleNode.get("lastUpdatedTime").longValue());
            }
            if (scheduleNode.path("interval").isTextual()) {
                scheduleBuilder.setInterval(scheduleNode.get("interval").textValue());
            }
            if (scheduleNode.path("isPaused").isBoolean()) {
                scheduleBuilder.setIsPaused(scheduleNode.get("isPaused").booleanValue());
            }
            tmBuilder.setSchedule(scheduleBuilder.build());
        }
    }

    private static boolean isValidOidcCredentials(Transformation.FlatOidcCredentials oidcCredentials) {
        return StringUtils.isNotBlank(oidcCredentials.getClientId())
                && StringUtils.isNotBlank(oidcCredentials.getClientSecret())
                && StringUtils.isNotBlank(oidcCredentials.getTokenUri())
                && StringUtils.isNotBlank(oidcCredentials.getCdfProjectName());
    }
}
