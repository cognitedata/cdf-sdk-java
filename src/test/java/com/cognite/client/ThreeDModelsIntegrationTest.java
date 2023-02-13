package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import org.jetbrains.annotations.NotNull;
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

public class ThreeDModelsIntegrationTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD = 3;

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteThreeDModels() throws MalformedURLException {
         try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModels() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            List<ThreeDModel> listUpsert = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);

            Thread.sleep(2000); // wait for eventual consistency

            findList(startInstant, loggingPrefix, client, listUpsert);

            delete(startInstant, loggingPrefix, client, listUpsert);

        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteThreeDModels() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModels() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            List<ThreeDModel> listUpsert = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);

            Thread.sleep(2000); // wait for eventual consistency

            List<ThreeDModel> editedTdInput = update(startInstant, loggingPrefix, client, listUpsert);

            replace(startInstant, loggingPrefix, client, editedTdInput);

            Thread.sleep(3000); // wait for eventual consistency

            delete(startInstant, loggingPrefix, client, listUpsert);

        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditSameCallAndDeleteThreeDModels() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModels() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            List<ThreeDModel> listUpsert = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);

            List<ThreeDModel> editedTdInput = listUpsert.stream()
                    .map(td -> td.toBuilder()
                            .setName("new-value")
                            .clearMetadata()
                            .putMetadata("new-key", "new-value")
                            .build())
                    .collect(Collectors.toList());

            List<ThreeDModel> listUpsertAndUpdate = DataGenerator.generate3DModels(COUNT_TO_BE_CREATE_TD, dataSetId);
            listUpsertAndUpdate.addAll(editedTdInput);

            List<ThreeDModel> listUpsertNews = upSertThreeDModel(listUpsertAndUpdate, startInstant, loggingPrefix, client, dataSetId);

            Thread.sleep(2000); // wait for eventual consistency

            delete(startInstant, loggingPrefix, client, listUpsertNews);

        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteThreeDModels() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - listThreeDModels() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

        List<ThreeDModel> listUpsert = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);

        Thread.sleep(2000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start retrieving 3D Models.");
        List<Item> tdList = new ArrayList<>();
        listUpsert.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> tdList.add(item));

        List<ThreeDModel> retrievedTD = client.threeD().models().retrieve(tdList);
        LOG.info(loggingPrefix + "Finished retrieving 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(listUpsert.size(), retrievedTD.size());

        delete(startInstant, loggingPrefix, client, listUpsert);
    }

    @Test
    @Tag("remoteCDP")
    void filter3DModelsWithPublished() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModels() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            List<ThreeDModel> listUpsert = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading 3D Models.");
            Request request = Request.create()
                    .withRootParameter("published", true);
            List<ThreeDModel> listResults = new ArrayList<>();
            client.threeD().models().list(request).forEachRemaining(model -> listResults.addAll(model));
            LOG.info(loggingPrefix + "------------ Finished reading 3D Models. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));

            assertNotNull(listResults.size());

            delete(startInstant, loggingPrefix, client, listUpsert);

        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private CogniteClient replace(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> editedTdInput) throws Exception {
        LOG.info(loggingPrefix + "Start update replace 3D Models.");
        client = client
                .withClientConfig(ClientConfig.create()
                        .withUpsertMode(UpsertMode.REPLACE));

        List<ThreeDModel> tdReplaceResults = client.threeD().models().upsert(editedTdInput);
        LOG.info(loggingPrefix + "Finished update replace 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        return client;
    }

    @NotNull
    private List<ThreeDModel> update(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
        LOG.info(loggingPrefix + "Start updating 3D Models.");
        List<ThreeDModel> editedTdInput = listUpsert.stream()
                .map(td -> td.toBuilder()
                        .setName("new-value")
                        .clearMetadata()
                        .putMetadata("new-key", "new-value")
                        .build())
                .collect(Collectors.toList());

        List<ThreeDModel> tdUpdateResults = client.threeD().models().upsert(editedTdInput);
        LOG.info(loggingPrefix + "Finished updating 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(editedTdInput.size(), tdUpdateResults.size());
        return editedTdInput;
    }

    private void delete(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
        LOG.info(loggingPrefix + "Start deleting 3D Models.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listUpsert.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.threeD().models().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    private List<ThreeDModel> findList(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
        LOG.info(loggingPrefix + "Start reading 3D Models.");
        List<ThreeDModel> listResults = new ArrayList<>();
        client.threeD().models().list().forEachRemaining(model -> listResults.addAll(model));
        LOG.info(loggingPrefix + "------------ Finished reading 3D Models. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertTrue(listResults.containsAll(listUpsert));
        return listResults;
    }

    @NotNull
    private List<ThreeDModel> createThreeDModel(Instant startInstant, String loggingPrefix, CogniteClient client, Long dataSetId) throws Exception {
        LOG.info(loggingPrefix + "------------ Start create 3D Models. ------------------");
        List<ThreeDModel> upsertThreeDModelsList = DataGenerator.generate3DModels(COUNT_TO_BE_CREATE_TD, dataSetId);
        List<ThreeDModel> listUpsert = client.threeD().models().upsert(upsertThreeDModelsList);
        LOG.info(loggingPrefix + "------------ Finished creating 3D Models. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;
    }

    @NotNull
    private List<ThreeDModel> upSertThreeDModel(List<ThreeDModel> upsertThreeDModelsList, Instant startInstant, String loggingPrefix, CogniteClient client, Long dataSetId) throws Exception {
        LOG.info(loggingPrefix + "------------ Start upserting 3D Models. ------------------");
        List<ThreeDModel> listUpsert = client.threeD().models().upsert(upsertThreeDModelsList);
        LOG.info(loggingPrefix + "------------ Finished upserting 3D Models. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;
    }

    @NotNull
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

    private CogniteClient getCogniteClient(Instant startInstant, String loggingPrefix) throws MalformedURLException {
        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        return client;
    }


}
