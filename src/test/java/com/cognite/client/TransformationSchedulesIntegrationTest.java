package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Item;
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

public class TransformationSchedulesIntegrationTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD = 1;

    @Test
    @Tag("remoteCDP")
    void writeListScheduleAndDelete() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeListScheduleAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);
            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start creating Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List =
                    DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, Transformation.Destination.DestinationType.DATA_SOURCE_1, 2,
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                            TestConfigProvider.getProject());
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            for (Transformation trans : listToBeCreate) {
                LOG.info(loggingPrefix + "------------ Start creating Transformations Schedules ------------------");
                List<Transformation.Schedule> listNewSchedules =
                        DataGenerator.generateTransformationSchedules(COUNT_TO_BE_CREATE_TD, trans.getExternalId(), "*/5 * * * *", false);

                List<Transformation.Schedule> createdListSchedules =
                        client.transformation().schedules().schedule(listNewSchedules);
                assertEquals(listNewSchedules.size(), createdListSchedules.size());
                LOG.info(loggingPrefix + "Finished creating Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                LOG.info(loggingPrefix + "------------ Starting listing Transformations Schedules ------------------");
                List<Transformation.Schedule> listSchedules = new ArrayList<>();
                Iterator<List<Transformation.Schedule>> it = client.transformation().schedules().list();
                it.forEachRemaining(val -> listSchedules.addAll(val));
                LOG.info(loggingPrefix + "Finished listing Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));
            }

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
    void writeEditScheduleRetrieveAndDelete() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeEditScheduleRetrieveAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);
            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start creating Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List =
                    DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, Transformation.Destination.DestinationType.DATA_SOURCE_1, 2,
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                            TestConfigProvider.getProject());
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            for (Transformation trans : listToBeCreate) {
                LOG.info(loggingPrefix + "------------ Start creating Transformations Schedules ------------------");
                List<Transformation.Schedule> listNewSchedules =
                        DataGenerator.generateTransformationSchedules(COUNT_TO_BE_CREATE_TD, trans.getExternalId(), "*/5 * * * *", false);

                List<Transformation.Schedule> createdListSchedules =
                        client.transformation().schedules().schedule(listNewSchedules);
                assertEquals(listNewSchedules.size(), createdListSchedules.size());
                LOG.info(loggingPrefix + "Finished creating Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                LOG.info(loggingPrefix + "Start updating Transformation Schedules.");
                List<Transformation.Schedule> editedInput = createdListSchedules.stream()
                        .map(sche -> sche.toBuilder()
                                .setIsPaused(true)
                                .setInterval("*/10 * * * *")
                                .build())
                        .collect(Collectors.toList());

                List<Transformation.Schedule> updatedList =
                        client.transformation().schedules().schedule(editedInput);
                assertEquals(createdList.size(), updatedList.size());
                updatedList.forEach(updated -> {
                    assertTrue(Boolean.TRUE.equals(updated.getIsPaused()));
                    assertTrue("*/10 * * * *".equals(updated.getInterval()));
                });

                LOG.info(loggingPrefix + "Finished updating Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                LOG.info(loggingPrefix + "------------ Starting retrieving Transformations Schedules ------------------");

                List<Item> itemsToRetrieve = new ArrayList<>();
                createdList.stream()
                        .map(tra -> Item.newBuilder()
                                .setExternalId(tra.getExternalId())
                                .build())
                        .forEach(item -> itemsToRetrieve.add(item));
                List<Transformation.Schedule> retrievedItems =
                        client.transformation().schedules().retrieve(itemsToRetrieve);
                assertNotNull(retrievedItems);
                assertTrue(retrievedItems.size()>0);
                assertEquals(createdListSchedules.size(), retrievedItems.size());

                LOG.info(loggingPrefix + "Finished retrieving Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));
            }

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
    void writeScheduleUnScheduleAndDelete() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeScheduleUnScheduleAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);
            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start creating Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List =
                    DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, Transformation.Destination.DestinationType.DATA_SOURCE_1, 2,
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                            TestConfigProvider.getProject());
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            for (Transformation trans : listToBeCreate) {
                LOG.info(loggingPrefix + "------------ Start creating Transformations Schedules ------------------");
                List<Transformation.Schedule> listNewSchedules =
                        DataGenerator.generateTransformationSchedules(COUNT_TO_BE_CREATE_TD, trans.getExternalId(), "*/5 * * * *", false);

                List<Transformation.Schedule> createdListSchedules =
                        client.transformation().schedules().schedule(listNewSchedules);
                assertEquals(listNewSchedules.size(), createdListSchedules.size());
                LOG.info(loggingPrefix + "Finished creating Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                LOG.info(loggingPrefix + "------------ Starting retrieving Transformations Schedules ------------------");

                List<Item> itemsToRetrieve = new ArrayList<>();
                createdList.stream()
                        .map(tra -> Item.newBuilder()
                                .setExternalId(tra.getExternalId())
                                .build())
                        .forEach(item -> itemsToRetrieve.add(item));
                List<Transformation.Schedule> retrievedItems =
                        client.transformation().schedules().retrieve(itemsToRetrieve);
                assertNotNull(retrievedItems);
                assertTrue(retrievedItems.size()>0);
                assertEquals(createdListSchedules.size(), retrievedItems.size());

                LOG.info(loggingPrefix + "Finished retrieving Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                LOG.info(loggingPrefix + "------------ Start unscheduling Transformations Schedules ------------------");
                List<Item> deleteScheduleItemsInput = new ArrayList<>();
                createdListSchedules.stream()
                        .map(tra -> Item.newBuilder()
                                .setExternalId(tra.getExternalId())
                                .build())
                        .forEach(item -> deleteScheduleItemsInput.add(item));

                Boolean isUnSchedule =
                        client.transformation().schedules().unSchedule(deleteScheduleItemsInput);
                assertTrue(isUnSchedule);
                LOG.info(loggingPrefix + "Finished unscheduling Transformations Schedules. Duration: {}",
                        Duration.between(startInstant, Instant.now()));

                createdList.stream()
                        .map(tra -> Item.newBuilder()
                                .setExternalId(tra.getExternalId())
                                .build())
                        .forEach(item -> itemsToRetrieve.add(item));
                retrievedItems =
                        client.transformation().schedules().retrieve(itemsToRetrieve);

                assertTrue(retrievedItems.size()==0);
            }

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
