package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.EntityMatchModel;
import com.cognite.client.dto.EntityMatchResult;
import com.cognite.client.dto.Item;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EntityMatchingIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void matchEntitiesStruct() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - matchEntitiesStruct() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = TestConfigProvider.getCogniteClient()
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start creating matching model.");
            String[] modelTypes = {"simple", "insensitive", "bigram", "frequencyweightedbigram",
                    "bigramextratokenizers", "bigramcombo"};
            long modelId = trainMatchingModel(client, modelTypes[1], loggingPrefix);

            LOG.info(loggingPrefix + "Finished creating matching model. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start entity match predict.");
            List<Struct> source = generateSourceStructs();
            List<Struct> target = generateTargetStructs();
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
                    .delete(List.of(modelItem));

            LOG.info(loggingPrefix + "Finished delete matching model. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            assertEquals(source.size(), matchResults.size());
            assertEquals(deleteResults.size(), 1);
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private long trainMatchingModel(CogniteClient client, String featureType, String loggingPrefix) throws Exception {
        // Set up the main data objects to use during the test
        List<Struct> source = generateSourceStructs();
        List<Struct> target = generateTargetTrainingStructs();

        // Train the matching model
        Request entityMatchFitRequest = Request.create()
                .withRootParameter("sources",  source)
                .withRootParameter("targets", target)
                .withRootParameter("matchFields", List.of(Map.of("source", "name", "target", "externalId")))
                .withRootParameter("featureType", featureType);

        List<EntityMatchModel> models = client.contextualization().entityMatching()
                .create(List.of(entityMatchFitRequest));

        LOG.debug(loggingPrefix + "Train matching model response body: {}",
                models.get(0));

        return models.get(0).getId();
    }

    private List<Struct> generateSourceStructs() {
        Struct entityA = Struct.newBuilder()
                .putFields("id", Values.of(1D))
                .putFields("name", Values.of("23-DB-9101"))
                .putFields("fooField", Values.of("bar"))
                .build();
        Struct entityB = Struct.newBuilder()
                .putFields("id", Values.of(2D))
                .putFields("name", Values.of("23-PC-9101"))
                .putFields("barField", Values.of("foo"))
                .build();
        Struct entityC = Struct.newBuilder()
                .putFields("id", Values.of(3D))
                .putFields("name", Values.of("343-Å"))
                .build();
        return List.of(entityA, entityB, entityC);
    }

    private List<Struct> generateTargetTrainingStructs() {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Values.of(1D))
                .putFields("externalId", Values.of("IA-23_DB_9101"))
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Values.of(2D))
                .putFields("externalId", Values.of("VAL_23_PC_9101"))
                .build();
        return List.of(targetA, targetB);
    }

    private List<Struct> generateTargetStructs() {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Values.of(1D))
                .putFields("externalId", Values.of("IA-23_DB_9101"))
                .putFields("uuid", Values.of(UUID.randomUUID().toString()))
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Values.of(2D))
                .putFields("externalId", Values.of("VAL_23_PC_9101"))
                .putFields("uuid", Values.of(UUID.randomUUID().toString()))
                .build();
        return List.of(targetA, targetB);
    }
}