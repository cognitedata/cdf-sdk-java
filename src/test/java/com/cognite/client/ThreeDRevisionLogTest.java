package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.ByteString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThreeDRevisionLogTest {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD_Revisions = 2;

    @Test
    @Tag("remoteCDP")
    void listThreeDRevisionLogs() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModelsRevisions() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);
            List<ThreeDModel> listUpsert3D = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);
            FileMetadata file = uploadFile();

            Map<ThreeDModel, List<ThreeDModelRevision>> map = new HashMap<>();
            List<ThreeDModelRevision> listUpsertRevisions = null;
            List<ThreeDModelRevision> listAllRevisions = new ArrayList<>();
            for (ThreeDModel model : listUpsert3D) {
                listUpsertRevisions = new ArrayList<>();
                listUpsertRevisions.addAll(createThreeDModelRevisions(startInstant, loggingPrefix, client, model, file));
                listAllRevisions.addAll(listUpsertRevisions);
                map.put(model, listUpsertRevisions);
            }
            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start list 3D Revision Logs.");

            List<ThreeDRevisionLog> listResultsLogs = new ArrayList<>();
            for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : map.entrySet()) {
                ThreeDModel model = entry.getKey();
                for (ThreeDModelRevision revision : entry.getValue()) {
                    List<ThreeDRevisionLog> listResults =
                            client.threeD()
                                    .models()
                                    .revisions()
                                    .revisionLogs()
                                    .retrieve(model.getId(), revision.getId());
                    listResultsLogs.addAll(listResults);
                }
            }

            assertEquals(listAllRevisions.size(), listResultsLogs.size());
            delete(startInstant, loggingPrefix, client, map, file);
            LOG.info(loggingPrefix + "Finished list 3D Revision Logs. Duration : {}",
                    Duration.between(startInstant, Instant.now()));
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }

    }

    private CogniteClient getCogniteClient(Instant startInstant, String loggingPrefix) throws MalformedURLException {
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
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

    @NotNull
    private List<ThreeDModel> createThreeDModel(Instant startInstant, String loggingPrefix, CogniteClient client, Long dataSetId) throws Exception {
        LOG.info(loggingPrefix + "------------ Start create 3D Models. ------------------");
        List<ThreeDModel> upsertThreeDModelsList = DataGenerator.generate3DModels(COUNT_TO_BE_CREATE_TD_Revisions, dataSetId);
        List<ThreeDModel> listUpsert = client.threeD().models().upsert(upsertThreeDModelsList);
        LOG.info(loggingPrefix + "------------ Finished creating 3D Models. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;
    }

    private FileMetadata uploadFile() throws MalformedURLException {
        Instant startInstant = Instant.now();
        Path fileAOriginal = Paths.get("./src/test/resources/CAMARO.obj");
        Path fileATemp = Paths.get("./tempA.tmp");
        byte[] fileByteA = new byte[0];
        try {
            // copy into temp path
            java.nio.file.Files.copy(fileAOriginal, fileATemp, StandardCopyOption.REPLACE_EXISTING);
            //
            fileByteA = java.nio.file.Files.readAllBytes(fileAOriginal);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<FileMetadata> fileMetadataList = DataGenerator.generateFileHeader3DModelsRevisions(1);
        List<FileContainer> fileContainerInput = new ArrayList<>();
        for (FileMetadata fileMetadata:  fileMetadataList) {
            FileContainer fileContainer = FileContainer.newBuilder()
                    .setFileMetadata(fileMetadata)
                    .setFileBinary(FileBinary.newBuilder()
                            .setBinary(ByteString.copyFrom(fileByteA)))
                    .build();
            fileContainerInput.add(fileContainer);

            ClientConfig config = ClientConfig.create()
                    .withNoWorkers(1)
                    .withNoListPartitions(1);

            CogniteClient client = CogniteClient.ofClientCredentials(
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
                    .withBaseUrl(TestConfigProvider.getHost());

            try {
                List<FileMetadata> uploadFileResult = client.files().upload(fileContainerInput);
                return uploadFileResult.get(0);
            } catch (Exception e) {
                LOG.error(e.toString());
                e.printStackTrace();
            }
        }

        return null;
    }

    private List<ThreeDModelRevision> createThreeDModelRevisions(Instant startInstant, String loggingPrefix, CogniteClient client, ThreeDModel threeDModel, FileMetadata file) throws Exception {
        LOG.info(loggingPrefix + "------------ Start create 3D Models Revisions. ------------------");
        List<ThreeDModelRevision> upsertThreeDModelsList = DataGenerator.generate3DModelsRevisions(COUNT_TO_BE_CREATE_TD_Revisions, file.getId());
        List<ThreeDModelRevision> listUpsert =
                client.threeD().models().revisions().upsert(threeDModel.getId(), upsertThreeDModelsList);
        LOG.info(loggingPrefix + "------------ Finished creating 3D Models Revisions. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;

    }

    private void delete(Instant startInstant, String loggingPrefix, CogniteClient client, Map<ThreeDModel, List<ThreeDModelRevision>> map, FileMetadata file) throws Exception {
        deleteFile(startInstant, loggingPrefix, client, file);
        deleteRevision(startInstant, loggingPrefix, client, map);
        List<ThreeDModel> listModels = map.keySet().stream()
                .collect(Collectors.toList());
        deleteThreeDModel(startInstant, loggingPrefix, client, listModels);
    }

    private void deleteFile(Instant startInstant, String loggingPrefix, CogniteClient client, FileMetadata file) throws Exception {
        LOG.info(loggingPrefix + "Start deleting files.");
        Item item = Item.newBuilder()
                .setExternalId(file.getExternalId())
                .build();
        List<Item> itens = List.of(item);
        List<Item> deleteItemsResults = client.files().delete(itens);
        LOG.info(loggingPrefix + "Finished deleting files. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(itens.size(), deleteItemsResults.size());
    }

    private void deleteRevision(Instant startInstant, String loggingPrefix, CogniteClient client, Map<ThreeDModel, List<ThreeDModelRevision>> map) throws Exception {
        LOG.info(loggingPrefix + "Start deleting 3D Model Revisions.");
        List<Item> deleteItemsInput = new ArrayList<>();
        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : map.entrySet()) {
            ThreeDModel model = entry.getKey();
            List<ThreeDModelRevision> listUpsertRevisions = entry.getValue();
            listUpsertRevisions.stream()
                    .map(td -> Item.newBuilder()
                            .setId(td.getId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));
            List<Item> deleteItemsResults = client.threeD().models().revisions().delete(model.getId(), deleteItemsInput);
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            deleteItemsInput.clear();
        }
        LOG.info(loggingPrefix + "Finished deleting 3D Model Revisions. Duration: {}",
                Duration.between(startInstant, Instant.now()));

    }

    private void deleteThreeDModel(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
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
}
