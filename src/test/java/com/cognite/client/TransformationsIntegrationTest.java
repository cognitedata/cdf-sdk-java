package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.TimeseriesMetadata;
import com.cognite.client.dto.Transformation;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationsIntegrationTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD = 1;

    @Test
    @Tag("remoteCDP")
    void list() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDelete() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        List<Transformation> listTransformationResults = new ArrayList<>();
        client.transformation()
                .list(Request.create()
                        .withFilterParameter("isPublic", true))
                .forEachRemaining(tra -> listTransformationResults.addAll(tra));
    }

    @Test
    @Tag("remoteCDP")
    void retrieve() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDelete() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        List<Transformation> listTransformationResults = new ArrayList<>();
        client.transformation()
                .list(Request.create()
                        .withFilterParameter("isPublic", true))
                .forEachRemaining(tra -> listTransformationResults.addAll(tra));

        List<Item> tdList = new ArrayList<>();
        listTransformationResults.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> tdList.add(item));

        client.transformation().retrieve(tdList);
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDelete() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeReadAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start create Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 1, 1);
            List<Transformation> generatedWithDestinationRawDataSourceList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 2, 1);
            List<Transformation> generatedWithDestinationSequenceRowList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 3, 2);
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);
            listToBeCreate.addAll(generatedWithDestinationSequenceRowList);

            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            Thread.sleep(20000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading Transformations.");
            List<Transformation> listTransformationResults = new ArrayList<>();
            client.transformation()
                    .list(Request.create()
                            .withFilterParameter("name", "TransformationTestSDK"))
                    .forEachRemaining(tra -> listTransformationResults.addAll(tra));
            LOG.info(loggingPrefix + "Finished reading Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertTrue(listTransformationResults.size()>0);
            createdList.forEach(created -> assertTrue(listTransformationResults.contains(created)));

            LOG.info(loggingPrefix + "Start deleting Transformations.");
            List<Item> deleteItemsInput = new ArrayList<>();
            createdList.stream()
                    .map(tra -> Item.newBuilder()
                            .setExternalId(tra.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));
            List<Item> deleteItemsResults = client.transformation().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeReadEditAndDelete() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeReadAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start create Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 1, 1);
            List<Transformation> generatedWithDestinationRawDataSourceList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 2, 1);
            List<Transformation> generatedWithDestinationSequenceRowList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 3, 2);
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);
            listToBeCreate.addAll(generatedWithDestinationSequenceRowList);

            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            LOG.info(loggingPrefix + "Start reading Transformations.");
            List<Transformation> listTransformationResults = new ArrayList<>();
            client.transformation()
                    .list(Request.create()
                            .withFilterParameter("name", "TransformationTestSDK"))
                    .forEachRemaining(tra -> listTransformationResults.addAll(tra));
            LOG.info(loggingPrefix + "Finished reading Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertTrue(listTransformationResults.size()>0);
            createdList.forEach(created -> assertTrue(listTransformationResults.contains(created)));

            LOG.info(loggingPrefix + "Start updating Transformations.");
            List<Transformation> editedInput = createdList.stream()
                    .map(tran -> tran.toBuilder()
                            .setQuery("new-value for update")
                            .build())
                    .collect(Collectors.toList());

            List<Transformation> updatedList = client.transformation().upsert(editedInput);
            assertEquals(createdList.size(), updatedList.size());
            updatedList.forEach(updated -> assertTrue(updated.getQuery().equals("new-value for update")));

            LOG.info(loggingPrefix + "Finished upserting Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting Transformations.");
            List<Item> deleteItemsInput = new ArrayList<>();
            createdList.stream()
                    .map(tra -> Item.newBuilder()
                            .setExternalId(tra.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));
            List<Item> deleteItemsResults = client.transformation().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDelete() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDelete() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

        LOG.info(loggingPrefix + "------------ Start create Transformations. ------------------");
        List<Transformation> listToBeCreate = new ArrayList<>();
        List<Transformation> generatedWithDestinationDataSource1List = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 1, 1);
        List<Transformation> generatedWithDestinationRawDataSourceList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 2, 1);
        List<Transformation> generatedWithDestinationSequenceRowList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, 3, 2);
        listToBeCreate.addAll(generatedWithDestinationDataSource1List);
        listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);
        listToBeCreate.addAll(generatedWithDestinationSequenceRowList);

        List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
        LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(listToBeCreate.size(), createdList.size());

        LOG.info(loggingPrefix + "Start retrieving Transformations.");
        List<Item> tdList = new ArrayList<>();
        createdList.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> tdList.add(item));

        List<Transformation> retrieved = client.transformation().retrieve(tdList);

        LOG.info(loggingPrefix + "Finished retrieving Transformations. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(createdList.size(), retrieved.size());

        LOG.info(loggingPrefix + "Start deleting Transformations.");
        List<Item> deleteItemsResults = client.transformation().delete(tdList);
        LOG.info(loggingPrefix + "Finished deleting Transformations. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(retrieved.size(), deleteItemsResults.size());
    }

    private CogniteClient getCogniteClient(Instant startInstant, String loggingPrefix) throws MalformedURLException {
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        return client;
    }

    private Long getOrCreateDataSet(Instant startInstant, String loggingPrefix, CogniteClient client) throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 1);
        Iterator<List<DataSet>> itDataSet = client.datasets().list(request);
        Long dataSetId = null;
        List<DataSet> list = itDataSet.next();
        if (list != null && list.size() > 0) {
            dataSetId = list.get(0).getId();
        } else {
            LOG.info(loggingPrefix + "------------ Start create or find one data set. ------------------");
            List<DataSet> upsertDataSetList = DataGenerator.generateDataSets(1);
            List<DataSet> upsertDataSetsResults = client.datasets().upsert(upsertDataSetList);
            dataSetId = upsertDataSetsResults.get(0).getId();
            LOG.info(loggingPrefix + "----------- Finished upserting data set. Duration: {} -------------",
                    Duration.between(startInstant, Instant.now()));
        }
        return dataSetId;
    }
}
