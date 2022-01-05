package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.ByteString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class ThreeDBaseTest {

    private static final Integer COUNT_TO_BE_CREATE = 2;

    abstract Logger getLogger();

    protected Map<ThreeDModel, List<ThreeDModelRevision>> map3D = new HashMap<>();
    protected CogniteClient client = null;
    protected FileMetadata file3D = null;

    @BeforeAll
    public void init() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - Preparing basic framework for tests -";
        client = getCogniteClient(loggingPrefix);

        Long dataSetId = getOrCreateDataSet(loggingPrefix, client);
        List<ThreeDModel> listUpsert3D = createThreeDModel(loggingPrefix, client, dataSetId);
        file3D = uploadFile();

        List<ThreeDModelRevision> listUpsertRevisions = null;
        List<ThreeDModelRevision> listAllRevisions = new ArrayList<>();
        for (ThreeDModel model : listUpsert3D) {
            listUpsertRevisions = new ArrayList<>();
            listUpsertRevisions.addAll(createThreeDModelRevisions(loggingPrefix, client, model, file3D));
            listAllRevisions.addAll(listUpsertRevisions);
            map3D.put(model, listUpsertRevisions);
        }
        Thread.sleep(2000); // wait for eventual consistency
    }

    @AfterAll
    public void end() throws Exception {
        String loggingPrefix = "UnitTest - Finished basic framework for tests -";
        delete(loggingPrefix, client, map3D, file3D);
    }

    private CogniteClient getCogniteClient(String loggingPrefix) throws MalformedURLException {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        getLogger().info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        return client;
    }

    @NotNull
    private Long getOrCreateDataSet(String loggingPrefix, CogniteClient client) throws Exception {
        Instant startInstant = Instant.now();
        Request request = Request.create()
                .withRootParameter("limit", 1);
        Iterator<List<DataSet>> itDataSet = client.datasets().list(request);
        Long dataSetId = null;
        List<DataSet> list = itDataSet.next();
        if (list != null && list.size() > 0) {
            dataSetId = list.get(0).getId();
        } else {
            getLogger().info(loggingPrefix + "------------ Start create or find one data set. ------------------");
            List<DataSet> upsertDataSetList = DataGenerator.generateDataSets(1);
            List<DataSet> upsertDataSetsResults = client.datasets().upsert(upsertDataSetList);
            dataSetId = upsertDataSetsResults.get(0).getId();
            getLogger().info(loggingPrefix + "----------- Finished upserting data set. Duration: {} -------------",
                    Duration.between(startInstant, Instant.now()));
        }
        return dataSetId;
    }

    @NotNull
    private List<ThreeDModel> createThreeDModel(String loggingPrefix, CogniteClient client, Long dataSetId) throws Exception {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "------------ Start create 3D Models. ------------------");
        List<ThreeDModel> upsertThreeDModelsList = DataGenerator.generate3DModels(COUNT_TO_BE_CREATE, dataSetId);
        List<ThreeDModel> listUpsert = client.threeD().models().upsert(upsertThreeDModelsList);
        getLogger().info(loggingPrefix + "------------ Finished creating 3D Models. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;
    }

    private FileMetadata uploadFile() throws MalformedURLException {
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

//            ClientConfig config = ClientConfig.create()
//                    .withNoWorkers(1)
//                    .withNoListPartitions(1);

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
                getLogger().error(e.toString());
                e.printStackTrace();
            }
        }

        return null;
    }

    private List<ThreeDModelRevision> createThreeDModelRevisions(String loggingPrefix, CogniteClient client, ThreeDModel threeDModel, FileMetadata file) throws Exception {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "------------ Start create 3D Models Revisions. ------------------");
        List<ThreeDModelRevision> upsertThreeDModelsList = DataGenerator.generate3DModelsRevisions(COUNT_TO_BE_CREATE, file.getId());
        List<ThreeDModelRevision> listUpsert =
                client.threeD().models().revisions().upsert(threeDModel.getId(), upsertThreeDModelsList);
        getLogger().info(loggingPrefix + "------------ Finished creating 3D Models Revisions. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;

    }

    private void delete(String loggingPrefix, CogniteClient client, Map<ThreeDModel, List<ThreeDModelRevision>> map, FileMetadata file) throws Exception {
        deleteFile(loggingPrefix, client, file);
        deleteRevision(loggingPrefix, client, map);
        List<ThreeDModel> listModels = map.keySet().stream()
                .collect(Collectors.toList());
        deleteThreeDModel(loggingPrefix, client, listModels);
    }

    private void deleteFile(String loggingPrefix, CogniteClient client, FileMetadata file) throws Exception {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "Start deleting files.");
        Item item = Item.newBuilder()
                .setExternalId(file.getExternalId())
                .build();
        List<Item> itens = List.of(item);
        List<Item> deleteItemsResults = client.files().delete(itens);
        getLogger().info(loggingPrefix + "Finished deleting files. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(itens.size(), deleteItemsResults.size());
    }

    private void deleteRevision(String loggingPrefix, CogniteClient client, Map<ThreeDModel, List<ThreeDModelRevision>> map) throws Exception {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "Start deleting 3D Model Revisions.");
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
        getLogger().info(loggingPrefix + "Finished deleting 3D Model Revisions. Duration: {}",
                Duration.between(startInstant, Instant.now()));

    }

    private void deleteThreeDModel(String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
        Instant startInstant = Instant.now();
        getLogger().info(loggingPrefix + "Start deleting 3D Models.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listUpsert.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.threeD().models().delete(deleteItemsInput);
        getLogger().info(loggingPrefix + "Finished deleting 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }
}
