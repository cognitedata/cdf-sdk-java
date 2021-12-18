package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThreeDModelsRevisionsTest {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD_Revisions = 1;

    @Test
    @Tag("remoteCDP")
    void listThreeDModelsRevisions() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModelsRevisions() -";
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            List<ThreeDModelRevision> listResults = new ArrayList<>();
            client.threeDModelsRevisions()
                    .list(1690988571960158L)
                    .forEachRemaining(model -> listResults.addAll(model));
            LOG.info(loggingPrefix + "------------ Finished reading 3D Model Revisions. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeThreeDModelsRevisions() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - listThreeDModelsRevisions() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);
        List<ThreeDModel> listUpsert3D = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);
        FileMetadata file = uploadFile();
        List<ThreeDModelRevision> listUpsertRevisions = createThreeDModelRevisions(startInstant, loggingPrefix, client, listUpsert3D.get(0), file);

        Thread.sleep(2000); // wait for eventual consistency

        findList(startInstant, loggingPrefix, client, listUpsertRevisions, listUpsert3D.get(0));

        delete(startInstant, loggingPrefix, client, listUpsertRevisions, listUpsert3D.get(0), file);

        deleteThreeDModel(startInstant, loggingPrefix, client, listUpsert3D);

    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteThreeDModelsRevisions() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - listThreeDModelsRevisions() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

        Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);
        List<ThreeDModel> listUpsert3D = createThreeDModel(startInstant, loggingPrefix, client, dataSetId);
        FileMetadata file = uploadFile();
        List<ThreeDModelRevision> listUpsertRevisions = createThreeDModelRevisions(startInstant, loggingPrefix, client, listUpsert3D.get(0), file);

        Thread.sleep(2000); // wait for eventual consistency

        List<ThreeDModelRevision> editedTdInput = update(startInstant, loggingPrefix, client, listUpsertRevisions, listUpsert3D.get(0));

        replace(startInstant, loggingPrefix, client, editedTdInput, listUpsert3D.get(0));

        Thread.sleep(3000); // wait for eventual consistency

        delete(startInstant, loggingPrefix, client, listUpsertRevisions, listUpsert3D.get(0), file);

        deleteThreeDModel(startInstant, loggingPrefix, client, listUpsert3D);
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
        List<ThreeDModel> upsertThreeDModelsList = DataGenerator.generate3DModels(1, dataSetId);
        List<ThreeDModel> listUpsert =
                client.threeDModels().upsert(upsertThreeDModelsList);
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
                client.threeDModelsRevisions().upsert(threeDModel.getId(), upsertThreeDModelsList);
        LOG.info(loggingPrefix + "------------ Finished creating 3D Models Revisions. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        assertEquals(upsertThreeDModelsList.size(), listUpsert.size());
        return listUpsert;

    }

    private List<ThreeDModelRevision> findList(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModelRevision> listUpsert, ThreeDModel threeDModel) throws Exception {
        List<ThreeDModelRevision> listResults = new ArrayList<>();
        client.threeDModelsRevisions()
                .list(threeDModel.getId())
                .forEachRemaining(model -> listResults.addAll(model));
        LOG.info(loggingPrefix + "------------ Finished reading 3D Model Revisions. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));
        return listResults;
    }

    private void delete(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModelRevision> listUpsertRevisions, ThreeDModel threeDModel, FileMetadata file) throws Exception {
        deleteFile(startInstant, loggingPrefix, client, file);
        deleteRevision(startInstant, loggingPrefix, client, listUpsertRevisions, threeDModel);
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

    private void deleteRevision(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModelRevision> listUpsertRevisions, ThreeDModel threeDModel) throws Exception {
        LOG.info(loggingPrefix + "Start deleting 3D Model Revisions.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listUpsertRevisions.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.threeDModelsRevisions().delete(threeDModel.getId(), deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting 3D Model Revisions. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    private void deleteThreeDModel(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModel> listUpsert) throws Exception {
        LOG.info(loggingPrefix + "Start deleting 3D Models.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listUpsert.stream()
                .map(td -> Item.newBuilder()
                        .setId(td.getId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.threeDModels().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting 3D Models. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    private void replace(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModelRevision> editedTdInput, ThreeDModel threeDModel) throws Exception {
        LOG.info(loggingPrefix + "Start update replace 3D Models Revisions.");
        client = client
                .withClientConfig(ClientConfig.create()
                        .withUpsertMode(UpsertMode.REPLACE));

        List<ThreeDModelRevision> tdReplaceResults = client.threeDModelsRevisions().upsert(threeDModel.getId(), editedTdInput);
        LOG.info(loggingPrefix + "Finished update replace 3D Models Revisions. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(editedTdInput.size(), tdReplaceResults.size());
    }

    private List<ThreeDModelRevision> update(Instant startInstant, String loggingPrefix, CogniteClient client, List<ThreeDModelRevision> listUpsertRevisions, ThreeDModel threeDModel) throws Exception {
        LOG.info(loggingPrefix + "Start updating 3D Models Revisions.");
        Random random = new Random();
        List<ThreeDModelRevision> editedTdInput = new ArrayList<>();
        for (ThreeDModelRevision revi : listUpsertRevisions) {
            ThreeDModelRevision.Builder builder = revi.toBuilder();
            ThreeDModelRevision.Camera.Builder cameraBuilder = ThreeDModelRevision.Camera.newBuilder();
            cameraBuilder.addPosition(2.707411050796508);
            cameraBuilder.addPosition(-4.514726638793944);
            cameraBuilder.addPosition(1.5695604085922240);
            cameraBuilder.addTarget(0.0);
            cameraBuilder.addTarget(-0.002374999923631548);
            cameraBuilder.addTarget(1.5695604085922240);
            builder.setCamera(cameraBuilder.build());
            builder.clearRotation();
            builder.addRotation(random.nextInt(100) / 100.0);
            builder.addRotation(random.nextInt(100) / 100.0);
            builder.addRotation(random.nextInt(100) / 100.0);
            builder.setPublished(false);
            builder.setStatus("Done");
            builder.putMetadata("new-key", "new-value");
            editedTdInput.add(builder.build());
        }

        List<ThreeDModelRevision> tdUpdateResults =
                client.threeDModelsRevisions().upsert(threeDModel.getId(), editedTdInput);
        LOG.info(loggingPrefix + "Finished updating 3D Models Revisions. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertEquals(editedTdInput.size(), tdUpdateResults.size());
        return editedTdInput;
    }
}
