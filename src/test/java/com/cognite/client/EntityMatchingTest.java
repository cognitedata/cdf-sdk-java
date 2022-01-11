package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.EntityMatchModel;
import com.cognite.client.dto.EntityMatchResult;
import com.cognite.client.dto.Item;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EntityMatchingTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void matchEntitiesStruct() throws Throwable {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - matchEntitiesStruct() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");


        LOG.info(loggingPrefix + "Start creating matching model.");
        String[] modelTypes = {"simple", "insensitive", "bigram", "frequencyweightedbigram",
                "bigramextratokenizers", "bigramcombo"};
        long modelId = trainMatchingModel(client, modelTypes[1], loggingPrefix);

        LOG.info(loggingPrefix + "Finished creating matching model. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start entity match predict.");
        ImmutableList<Struct> source = generateSourceStructs();
        ImmutableList<Struct> target = generateTargetStructs();
        List<EntityMatchResult> matchResults = client.contextualization()
                .entityMatching()
                .predict(modelId, source, target);

        LOG.info(loggingPrefix + "Finished entity match predict. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start delete matching model.");
        Item modelItem = Item.newBuilder()
                .setId(modelId)
                .build();
        List<Item> deleteResults = client.contextualization()
                .entityMatching()
                .delete(ImmutableList.of(modelItem));

        LOG.info(loggingPrefix + "Finished delete matching model. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(source.size(), matchResults.size());
        assertEquals(deleteResults.size(), 1);

    }

    private long trainMatchingModel(CogniteClient client, String featureType, String loggingPrefix) throws Throwable {
        // Set up the main data objects to use during the test
        ImmutableList<Struct> source = generateSourceStructs();
        ImmutableList<Struct> target = generateTargetTrainingStructs();

        // Train the matching model
        Request entityMatchFitRequest = Request.create()
                .withRootParameter("sources", source)
                .withRootParameter("targets", target)
                .withRootParameter("matchFields", ImmutableList.of(
                        ImmutableMap.of("source", "name", "target", "externalId")
                ))
                .withRootParameter("featureType", featureType);

        List<EntityMatchModel> models = client.contextualization().entityMatching()
                .create(ImmutableList.of(entityMatchFitRequest));

        LOG.debug(loggingPrefix + "Train matching model response body: {}",
                models.get(0));

        return models.get(0).getId();
    }

    private ImmutableList<Struct> generateSourceStructs() throws Throwable {
        Struct entityA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("name", Value.newBuilder().setStringValue("23-DB-9101").build())
                .putFields("fooField", Value.newBuilder().setStringValue("bar").build())
                .build();
        Struct entityB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("name", Value.newBuilder().setStringValue("23-PC-9101").build())
                .putFields("barField", Value.newBuilder().setStringValue("foo").build())
                .build();
        Struct entityC = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(3D).build())
                .putFields("name", Value.newBuilder().setStringValue("343-Å").build())
                .build();
        return ImmutableList.of(entityA, entityB, entityC);
    }

    private ImmutableList<Struct> generateTargetTrainingStructs() throws Throwable {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
                .build();
        return ImmutableList.of(targetA, targetB);
    }

    private ImmutableList<Struct> generateTargetStructs() throws Throwable {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
                .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
                .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
                .build();
        return ImmutableList.of(targetA, targetB);
    }
}