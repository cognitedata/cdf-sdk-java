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

import static org.junit.jupiter.api.Assertions.*;

public class TransformationNotificationsIntegrationTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD = 1;

    @Test
    @Tag("remoteCDP")
    void subscribeListAndDeleteNotifications() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - subscribeListAndDeleteNotifications() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start create Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List =
                    DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, "data_source", 2,
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                            TestConfigProvider.getProject());
            List<Transformation> generatedWithDestinationRawDataSourceList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, "raw", 2,
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                    TestConfigProvider.getProject());
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);

            List<Transformation> createdList = client.transformations().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            LOG.info(loggingPrefix + "Start subscribing Notifications Transformations.");
            List<Transformation.Notification.Subscription> subscribes = new ArrayList<>();
            for (Transformation transformation : createdList) {
                subscribes.add(Transformation.Notification.Subscription.newBuilder()
                        .setDestination(transformation.getOwner().getUser()+"@test.com")
                        .setTransformationId(transformation.getId())
                        .build());
            }

            List<Transformation.Notification> createdSubscribes =
                    client.transformations().notifications().subscribe(subscribes);
            assertNotNull(createdSubscribes);
            assertTrue(createdSubscribes.size()>0);
            LOG.info(loggingPrefix + "Finished subscribing Notifications Transformations.");

            LOG.info(loggingPrefix + "Start linting Notifications Transformations.");
            List<Transformation.Notification> listNotifications = new ArrayList<>();
            Iterator<List<Transformation.Notification>> itNotifications =
                    client.transformations().notifications().list();
            itNotifications.forEachRemaining(it -> listNotifications.addAll(it));
            createdSubscribes.forEach(created -> assertTrue(listNotifications.contains(created)));
            LOG.info(loggingPrefix + "Finished linting Notifications Transformations.");

            LOG.info(loggingPrefix + "Start deleting Notifications Transformations.");
            List<Item> deleteNotificationsInput = new ArrayList<>();
            createdSubscribes.stream()
                    .map(sub -> Item.newBuilder()
                            .setId(sub.getId())
                            .build())
                    .forEach(item -> deleteNotificationsInput.add(item));
            List<Item> deletedItems = client.transformations().notifications().delete(deleteNotificationsInput);
            assertNotNull(deletedItems);
            assertTrue(deletedItems.size()>0);
            deleteNotificationsInput.forEach(toBeDeleted -> assertTrue(deletedItems.contains(toBeDeleted)));

            listNotifications.clear();
            itNotifications =
                    client.transformations().notifications().list();
            itNotifications.forEachRemaining(it -> listNotifications.addAll(it));
            createdSubscribes.forEach(created -> assertTrue(!listNotifications.contains(created)));

            LOG.info(loggingPrefix + "Finished deleting Notifications Transformations.");

            LOG.info(loggingPrefix + "Start deleting Transformations.");
            List<Item> deleteItemsInput = new ArrayList<>();
            createdList.stream()
                    .map(tra -> Item.newBuilder()
                            .setExternalId(tra.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));
            List<Item> deleteItemsResults = client.transformations().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private CogniteClient getCogniteClient(Instant startInstant, String loggingPrefix) throws MalformedURLException {
        CogniteClient client = TestConfigProvider.getCogniteClient();
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
