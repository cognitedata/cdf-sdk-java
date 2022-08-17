package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
import com.cognite.client.queue.UploadQueue;
import com.cognite.client.util.DataGenerator;
import com.google.common.collect.ImmutableMap;
import com.cognite.client.util.Items;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class FilesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void downloadValidateUri() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - downloadValidateUri() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
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

        LOG.info(loggingPrefix + "Check download URI validation.");
        assertThrows(Exception.class, () -> client.files()
                .download(Items.parseItems(1, 2, 3), new URI("notSupported://myBucket"), false));
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteFiles() throws Exception {
        Instant startInstant = Instant.now();
        Path fileAOriginal = Paths.get("./src/test/resources/csv-data.txt");
        Path fileATemp = Paths.get("./tempA.tmp");
        Path fileB = Paths.get("./src/test/resources/csv-data-bom.txt");
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        try {
            // copy into temp path
            java.nio.file.Files.copy(fileAOriginal, fileATemp, StandardCopyOption.REPLACE_EXISTING);
            //
            fileByteA = java.nio.file.Files.readAllBytes(fileAOriginal);
            fileByteB = java.nio.file.Files.readAllBytes(fileB);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<FileMetadata> fileMetadataList = DataGenerator.generateFileHeaderObjects(2);
        List<FileContainer> fileContainerInput = new ArrayList<>();
        for (FileMetadata fileMetadata : fileMetadataList) {
            FileContainer fileContainer = FileContainer.newBuilder()
                    .setFileMetadata(fileMetadata)
                    .setFileBinary(FileBinary.newBuilder()
                            .setBinary(ByteString.copyFrom(ThreadLocalRandom.current().nextBoolean() ? fileByteA : fileByteB)))
                    .build();
            fileContainerInput.add(fileContainer);
        }

        // add a file binary based on a URI
        FileContainer fileContainer = FileContainer.newBuilder()
                .setFileMetadata(DataGenerator.generateFileHeaderObjects(1).get(0))
                .setFileBinary(FileBinary.newBuilder()
//                        .setBinaryUri("s3://testbucket/README.md")
                                .setBinaryUri(fileATemp.toUri().toString())
                )
                .build();
        fileContainerInput.add(fileContainer);

        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteFiles() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
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

        try {
            LOG.info(loggingPrefix + "Start uploading file binaries.");
            List<FileMetadata> uploadFileResult = client.files().upload(fileContainerInput);
            LOG.info(loggingPrefix + "Finished uploading file binaries. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(5000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading file metadata.");
            List<FileMetadata> listFilesResults = new ArrayList<>();
            client.files()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(files -> listFilesResults.addAll(files));
            LOG.info(loggingPrefix + "Finished reading files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start reading file aggregates.");
            Aggregate fileAggregate = client.files().aggregate(Request.create()
                    .withFilterParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate : {}",
                    fileAggregate.toString());
            LOG.info(loggingPrefix + "Finished reading file aggregates. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start editing file metadata.");
            List<FileMetadata> editFilesResult = new ArrayList<>();
            List<FileMetadata> editFilesInput = listFilesResults.stream()
                    .map(fileMetadata -> fileMetadata.toBuilder()
                            .putMetadata("addedField", "new field value")
                            .build())
                    .collect(Collectors.toList());
            editFilesResult = client.files().upsert(editFilesInput);
            LOG.info(loggingPrefix + "Finished editing file metadata. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start downloading file binaries.");
            List<FileContainer> downloadFilesResults = new ArrayList<>();
            List<Item> downloadFilesItems = listFilesResults.stream()
                    .map(fileMetadata -> Item.newBuilder()
                            .setId(fileMetadata.getId())
                            .build())
                    .collect(Collectors.toList());
            downloadFilesResults = client.files().downloadToPath(downloadFilesItems, Paths.get(""));
            LOG.info(loggingPrefix + "Finished reading file binaries. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting files.");
            List<Item> deleteItemsInput = downloadFilesItems;
            List<Item> deleteItemsResults = client.files().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(fileContainerInput.size(), listFilesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());


        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteFilesNoBinary() throws Exception {
        Instant startInstant = Instant.now();

        List<FileMetadata> fileMetadataList = DataGenerator.generateFileHeaderObjects(2);
        List<FileContainer> fileContainerInput = new ArrayList<>();
        for (FileMetadata fileMetadata : fileMetadataList) {
            FileContainer fileContainer = FileContainer.newBuilder()
                    .setFileMetadata(fileMetadata)
                    .build();
            fileContainerInput.add(fileContainer);
        }
        String loggingPrefix = "UnitTest - writeReadAndDeleteFiles() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
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

        try {
            LOG.info(loggingPrefix + "Start uploading file with empty binaries.");
            List<FileMetadata> uploadFileResult = client.files().upload(fileContainerInput);
            LOG.info(loggingPrefix + "Finished uploading file with empty binaries. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(5000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading file metadata.");
            List<FileMetadata> listFilesResults = new ArrayList<>();
            client.files()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(files -> listFilesResults.addAll(files));
            LOG.info(loggingPrefix + "Finished reading file metadata. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting files.");
            List<Item> deleteItemsInput = listFilesResults.stream()
                    .map(fileMetadata -> Item.newBuilder()
                            .setExternalId(fileMetadata.getExternalId())
                            .build())
                    .collect(Collectors.toList());
            List<Item> deleteItemsResults = client.files().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(fileContainerInput.size(), listFilesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());

        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void readFilestoBucket() throws Exception {
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - readFiles() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start reading file metadata.");
            List<FileMetadata> listFilesResults = new ArrayList<>();
            client.files()
                    .list(Request.create()
                            .withRequestParameters(ImmutableMap.of("limit", 2)))
                    .forEachRemaining(listFilesResults::addAll);
            LOG.info(loggingPrefix + "Finished reading file metadata. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting files.");
            List<Item> readItems = listFilesResults.stream()
                    .map(fileMetadata -> Item.newBuilder()
                            .setId(fileMetadata.getId())
                            .build())
                    .collect(Collectors.toList());
            final List<FileBinary> fileBinaries =
                    client.files().downloadFileBinaries(readItems, URI.create("s3://testbucket"), true);

            fileBinaries.forEach(s -> LOG.info(String.format("File %s maps to %s", s.getBinaryUri(), s.getId())));
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeUploadQueueReadAndDeleteFiles() throws Exception {
        Instant startInstant = Instant.now();
        Path fileAOriginal = Paths.get("./src/test/resources/csv-data.txt");
        Path fileATemp = Paths.get("./tempA.tmp");
        Path fileB = Paths.get("./src/test/resources/csv-data-bom.txt");
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        try {
            // copy into temp path
            java.nio.file.Files.copy(fileAOriginal, fileATemp, StandardCopyOption.REPLACE_EXISTING);
            //
            fileByteA = java.nio.file.Files.readAllBytes(fileAOriginal);
            fileByteB = java.nio.file.Files.readAllBytes(fileB);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<FileMetadata> fileMetadataList = DataGenerator.generateFileHeaderObjects(2);
        List<FileContainer> fileContainerInput = new ArrayList<>();
        for (FileMetadata fileMetadata : fileMetadataList) {
            FileContainer fileContainer = FileContainer.newBuilder()
                    .setFileMetadata(fileMetadata)
                    .setFileBinary(FileBinary.newBuilder()
                            .setBinary(ByteString.copyFrom(ThreadLocalRandom.current().nextBoolean() ? fileByteA : fileByteB)))
                    .build();
            fileContainerInput.add(fileContainer);
        }

        // add a file binary based on a URI
        FileContainer fileContainer = FileContainer.newBuilder()
                .setFileMetadata(DataGenerator.generateFileHeaderObjects(1).get(0))
                .setFileBinary(FileBinary.newBuilder()
//                        .setBinaryUri("s3://testbucket/README.md")
                                .setBinaryUri(fileATemp.toUri().toString())
                )
                .build();
        fileContainerInput.add(fileContainer);

        String loggingPrefix = "UnitTest - writeReadAndDeleteFiles() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");


        LOG.info(loggingPrefix + "Start uploading file binaries.");
        UploadQueue<FileContainer, FileMetadata> fileBinaryUploadQueue = client.files().fileContainerUploadQueue()
                .withMaxUploadInterval(Duration.ofSeconds(5))
                .withPostUploadFunction(fileMetadata -> LOG.info("postUploadFunction triggered. Uploaded {} items", fileMetadata.size()))
                .withExceptionHandlerFunction(exception -> LOG.warn("exceptionHandlerFunction triggered: {}", exception.getMessage()));

        fileBinaryUploadQueue.start();
        for (FileContainer container : fileContainerInput) {
            fileBinaryUploadQueue.put(container);
        }
        fileBinaryUploadQueue.stop();
        LOG.info(loggingPrefix + "Finished uploading file binaries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(5000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading file metadata.");
        List<FileMetadata> listFilesResults = new ArrayList<>();
        client.files()
                .list(Request.create()
                        .withFilterParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(files -> listFilesResults.addAll(files));
        LOG.info(loggingPrefix + "Finished reading files. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start editing file metadata.");
        final List<FileMetadata> editFilesResult = new ArrayList<>();
        List<FileMetadata> editFilesInput = listFilesResults.stream()
                .map(fileMetadata -> fileMetadata.toBuilder()
                        .putMetadata("addedField", "new field value")
                        .build())
                .collect(Collectors.toList());

        UploadQueue<FileMetadata, FileMetadata> fileMetadataUploadQueue = client.files().metadataUploadQueue()
                .withMaxUploadInterval(Duration.ofSeconds(1))
                .withPostUploadFunction(files -> {
                    LOG.info("postUploadFunction triggered. Uploaded {} items", files.size());
                    editFilesResult.addAll(files);
                })
                .withExceptionHandlerFunction(exception -> LOG.warn("exceptionHandlerFunction triggered: {}", exception.getMessage()));
        fileMetadataUploadQueue.start();
        for (FileMetadata fileHeader : editFilesInput) {
            fileMetadataUploadQueue.put(fileHeader);
        }
        fileMetadataUploadQueue.stop();
        LOG.info(loggingPrefix + "Finished editing file metadata. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting files.");
        List<Item> deleteItemsInput = editFilesResult.stream()
                .map(fileMetadata -> Item.newBuilder().setId(fileMetadata.getId()).build())
                .collect(Collectors.toList());
        List<Item> deleteItemsResults = client.files().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting files. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(fileContainerInput.size(), listFilesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }
}