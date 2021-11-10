/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.executor.FileBinaryRequestExecutor;
import com.cognite.client.servicesV1.parser.FileParser;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite events api endpoint.
 *
 * It provides methods for reading and writing {@link Event}.
 */
@AutoValue
public abstract class Files extends ApiBase {
    private static final int MAX_WRITE_REQUEST_BATCH_SIZE = 100;
    private static final int MAX_DOWNLOAD_BINARY_BATCH_SIZE = 10;
    private static final int MAX_UPLOAD_BINARY_BATCH_SIZE = 10;

    private static Builder builder() {
        return new AutoValue_Files.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Files.class);

    /**
     * Constructs a new {@link Files} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Files of(CogniteClient client) {
        return Files.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link FileMetadata} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<FileMetadata>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link FileMetadata} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The assets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<FileMetadata>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Event} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<FileMetadata>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.FILE_HEADER, requestParameters, partitions), this::parseFileMetadata);
    }

    /**
     * Retrieve files by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved file headers.
     * @throws Exception
     */
    public List<FileMetadata> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.FILE_HEADER, items).stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * @param requestParameters The filtering and aggregates specification
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.FILE_HEADER, requestParameters);
    }

    /**
     * Creates or updates a set of {@link FileMetadata} objects.
     *
     * If it is a new {@link FileMetadata} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link FileMetadata} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param fileMetadataList The file headers / metadata to upsert.
     * @return The upserted file headers.
     * @throws Exception
     */
    public List<FileMetadata> upsert(List<FileMetadata> fileMetadataList) throws Exception {
        String loggingPrefix = "upsert() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        final int maxUpsertLoopIterations = 3;
        Instant startInstant = Instant.now();
        if (fileMetadataList.isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the read request.");
            return Collections.emptyList();
        }

        ConnectorServiceV1.ItemWriter updateWriter = getClient().getConnectorService().updateFileHeaders();
        ConnectorServiceV1.ItemWriter createWriter = getClient().getConnectorService().writeFileHeaders();

        // naive de-duplication based on ids
        Map<Long, FileMetadata> internalIdUpdateMap = new HashMap<>(1000);
        Map<String, FileMetadata> externalIdUpdateMap = new HashMap<>(1000);
        Map<Long, FileMetadata> internalIdAssetsMap = new HashMap<>(50);
        Map<String, FileMetadata> externalIdAssetsMap = new HashMap<>(50);
        for (FileMetadata value : fileMetadataList) {
            if (value.hasExternalId()) {
                externalIdUpdateMap.put(value.getExternalId(), value);
            } else if (value.hasId()) {
                internalIdUpdateMap.put(value.getId(), value);
            } else {
                throw new Exception("File metadata item does not contain id nor externalId: " + value.toString());
            }
        }

        // Check for files with >1k assets. Set the extra assets aside so we can add them in separate updates.
        for (Long key : internalIdUpdateMap.keySet()) {
            FileMetadata fileMetadata = internalIdUpdateMap.get(key);
            if (fileMetadata.getAssetIdsCount() > 1000) {
                internalIdUpdateMap.put(key, fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                        .build());
                internalIdAssetsMap.put(key, FileMetadata.newBuilder()
                        .setId(fileMetadata.getId())
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                        .build());
            }
        }
        for (String key : externalIdUpdateMap.keySet()) {
            FileMetadata fileMetadata = externalIdUpdateMap.get(key);
            if (fileMetadata.getAssetIdsCount() > 1000) {
                externalIdUpdateMap.put(key, fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                        .build());
                externalIdAssetsMap.put(key, FileMetadata.newBuilder()
                        .setExternalId(fileMetadata.getExternalId())
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                        .build());
            }
        }

        // Combine the input into list
        List<FileMetadata> elementListUpdate = new ArrayList<>();
        List<FileMetadata> elementListCreate = new ArrayList<>();
        List<String> elementListCompleted = new ArrayList<>();

        elementListUpdate.addAll(externalIdUpdateMap.values());
        elementListUpdate.addAll(internalIdUpdateMap.values());

        /*
        The upsert loop. If there are items left to insert or update:
        1. Update elements
        2. If conflicts move missing items into the insert maps
        3. Insert elements
        4. If conflict, remove duplicates into the update maps
        */
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String exceptionMessage = "";
        for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListUpdate.size()) > 0;
             i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.debug(loggingPrefix + "Start upsert loop {} with {} items to update, {} items to create and "
                            + "{} completed items at duration {}",
                    i,
                    elementListUpdate.size(),
                    elementListCreate.size(),
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            /*
            Update items
             */
            if (elementListUpdate.isEmpty()) {
                LOG.debug(loggingPrefix + "Update items list is empty. Skipping update.");
            } else {
                Map<ResponseItems<String>, List<FileMetadata>> updateResponseMap =
                        splitAndUpdateFileMetadata(elementListUpdate, updateWriter);
                LOG.debug(loggingPrefix + "Completed update items requests for {} items across {} batches at duration {}",
                        elementListUpdate.size(),
                        updateResponseMap.size(),
                        Duration.between(startInstant, Instant.now()).toString());
                elementListUpdate.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : updateResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        elementListCompleted.addAll(response.getResultsItems());
                        LOG.debug(loggingPrefix + "Update items request success. Adding {} update result items to result collection.",
                                response.getResultsItems().size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        LOG.debug(loggingPrefix + "Update items request failed: {}", response.getResponseBodyAsString());
                        if (i == maxUpsertLoopIterations - 1) {
                            // Add the error message to std logging
                            LOG.error(loggingPrefix + "Update items request failed. {}", response.getResponseBodyAsString());
                        }
                        LOG.debug(loggingPrefix + "Converting missing items to create and retrying the request");
                        List<Item> missing = ItemParser.parseItems(response.getMissingItems());
                        LOG.debug(loggingPrefix + "Number of missing entries reported by CDF: {}", missing.size());

                        // Move missing items from update to the create request
                        Map<String, FileMetadata> itemsMap = mapToId(updateResponseMap.get(response));
                        for (Item value : missing) {
                            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                                elementListCreate.add(itemsMap.get(value.getExternalId()));
                                itemsMap.remove(value.getExternalId());
                            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                                elementListCreate.add(itemsMap.get(String.valueOf(value.getId())));
                                itemsMap.remove(String.valueOf(value.getId()));
                            }
                        }
                        elementListUpdate.addAll(itemsMap.values()); // Add remaining items to be re-updated
                    }
                }
            }

            /*
            Insert / create items
             */
            if (elementListCreate.isEmpty()) {
                LOG.debug(loggingPrefix + "Create items list is empty. Skipping create.");
            } else {
                Map<ResponseItems<String>, FileMetadata> createResponseMap =
                        splitAndCreateFileMetadata(elementListCreate, createWriter);
                LOG.debug(loggingPrefix + "Completed create items requests for {} items across {} batches at duration {}",
                        elementListCreate.size(),
                        createResponseMap.size(),
                        Duration.between(startInstant, Instant.now()).toString());
                elementListCreate.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : createResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        elementListCompleted.addAll(response.getResultsItems());
                        LOG.debug(loggingPrefix + "Create items request success. Adding {} create result items to result collection.",
                                response.getResultsItems().size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        LOG.debug(loggingPrefix + "Create items request failed: {}", response.getResponseBodyAsString());
                        if (i == maxUpsertLoopIterations - 1) {
                            // Add the error message to std logging
                            LOG.error(loggingPrefix + "Create items request failed. {}", response.getResponseBodyAsString());
                        }
                        LOG.debug(loggingPrefix + "Converting duplicates to update and retrying the request");
                        List<Item> duplicates = ItemParser.parseItems(response.getDuplicateItems());
                        LOG.debug(loggingPrefix + "Number of duplicate entries reported by CDF: {}", duplicates.size());

                        // Move duplicates from insert to the update request
                        Map<String, FileMetadata> itemsMap = mapToId(ImmutableList.of(createResponseMap.get(response)));
                        for (Item value : duplicates) {
                            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                                elementListUpdate.add(itemsMap.get(value.getExternalId()));
                                itemsMap.remove(value.getExternalId());
                            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                                elementListUpdate.add(itemsMap.get(String.valueOf(value.getId())));
                                itemsMap.remove(String.valueOf(value.getId()));
                            }
                        }
                        elementListCreate.addAll(itemsMap.values()); // Add remaining items to be re-inserted
                    }
                }
            }
        }

        /*
        Write extra assets id links as separate updates. The api only supports 1k assetId links per file object
        per api request. If a file contains a large number of assetIds, we need to split them up into an initial
        file create/update (all the code above) and subsequent update requests which add the remaining
        assetIds (code below).
         */
        Map<Long, FileMetadata> internalIdTempMap = new HashMap<>();
        Map<String, FileMetadata> externalIdTempMap = new HashMap<>();
        List<FileMetadata> elementListAssetUpdate = new ArrayList<>();
        while (internalIdAssetsMap.size() > 0 || externalIdAssetsMap.size() > 0) {
            LOG.info(loggingPrefix + "Some files have very high assetId cardinality (+1k). Adding assetId to "
                    + (internalIdAssetsMap.size() + externalIdAssetsMap.size())
                    + " file(s).");
            internalIdUpdateMap.clear();
            externalIdUpdateMap.clear();
            internalIdTempMap.clear();
            externalIdTempMap.clear();

            // Check for files with >1k remaining assets
            for (Long key : internalIdAssetsMap.keySet()) {
                FileMetadata fileMetadata = internalIdAssetsMap.get(key);
                if (fileMetadata.getAssetIdsCount() > 1000) {
                    internalIdUpdateMap.put(key, fileMetadata.toBuilder()
                            .clearAssetIds()
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                            .build());
                    internalIdTempMap.put(key, FileMetadata.newBuilder()
                            .setId(fileMetadata.getId())
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                            .build());
                } else {
                    // The entire assetId list can be pushed in a single update
                    internalIdUpdateMap.put(key, fileMetadata);
                }
            }
            internalIdAssetsMap.clear();
            internalIdAssetsMap.putAll(internalIdTempMap);

            for (String key : externalIdAssetsMap.keySet()) {
                FileMetadata fileMetadata = externalIdAssetsMap.get(key);
                if (fileMetadata.getAssetIdsCount() > 1000) {
                    externalIdUpdateMap.put(key, fileMetadata.toBuilder()
                            .clearAssetIds()
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                            .build());
                    externalIdTempMap.put(key, FileMetadata.newBuilder()
                            .setExternalId(fileMetadata.getExternalId())
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                            .build());
                } else {
                    // The entire assetId list can be pushed in a single update
                    externalIdUpdateMap.put(key, fileMetadata);
                }
            }
            externalIdAssetsMap.clear();
            externalIdAssetsMap.putAll(externalIdTempMap);

            // prepare the update and send request
            LOG.info(loggingPrefix + "Building update request to add assetIds for {} files.",
                    internalIdUpdateMap.size() + externalIdUpdateMap.size());
            elementListAssetUpdate.clear();
            elementListAssetUpdate.addAll(externalIdUpdateMap.values());
            elementListAssetUpdate.addAll(internalIdUpdateMap.values());

            // should not happen, but need to check
            if (elementListAssetUpdate.isEmpty()) {
                String message = loggingPrefix + "Internal error. Not able to send assetId update. The payload is empty.";
                LOG.error(message);
                throw new Exception(message);
            }

            Map<ResponseItems<String>, List<FileMetadata>> responseItemsAssets =
                    splitAndAddAssets(elementListAssetUpdate, updateWriter);
            for (ResponseItems<String> responseItems : responseItemsAssets.keySet()) {
                if (!responseItems.isSuccessful()) {
                    String message = loggingPrefix
                            + "Failed to add assetIds. "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }
            }
        }

        // Check if all elements completed the upsert requests
        if (elementListCreate.isEmpty() && elementListUpdate.isEmpty()) {
            LOG.info(loggingPrefix + "Successfully upserted {} items within a duration of {}.",
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        } else {
            LOG.error(loggingPrefix + "Failed to upsert items. {} items remaining. {} items completed upsert."
                            + System.lineSeparator() + "{}",
                    elementListCreate.size() + elementListUpdate.size(),
                    elementListCompleted.size(),
                    exceptionMessage);
            throw new Exception(String.format(loggingPrefix + "Failed to upsert items. %d items remaining. "
                            + " %d items completed upsert. %n " + exceptionMessage,
                    elementListCreate.size() + elementListUpdate.size(),
                    elementListCompleted.size()));
        }



        return elementListCompleted.stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Uploads a set of file headers and binaries to Cognite Data Fusion.
     *
     * The file binary can either be placed in-memory in the file container (as a byte string)
     * or referenced (by URI) to a blob store.
     *
     * @param files The files to upload.
     * @return The file metadata / headers for the uploaded files.
     * @throws Exception
     */
    public List<FileMetadata> upload(List<FileContainer> files) throws Exception {
        return this.upload(files, false);
    }

    /**
     * Uploads a set of file headers and binaries to Cognite Data Fusion.
     *
     * The file binary can either be placed in-memory in the file container (as a byte string)
     * or referenced (by URI) to a blob store.
     *
     * In case you reference the file by URI, you can choose to automatically remove the file binary
     * from the (URI referenced) blob store after a successful upload to Cognite Data Fusion. This can
     * Be useful in situations where you perform large scala data transfers utilizing a temp backing
     * store.
     *
     * @param files The files to upload.
     * @param deleteTempFile Set to true to remove the URI binary after upload. Set to false to keep the URI binary.
     * @return The file metadata / headers for the uploaded files.
     * @throws Exception
     */
    public List<FileMetadata> upload(@NotNull List<FileContainer> files, boolean deleteTempFile) throws Exception {
        String loggingPrefix = "upload() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        if (files.isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the upload request.");
            return Collections.emptyList();
        }

        ConnectorServiceV1.FileWriter fileWriter = getClient().getConnectorService().writeFileProto()
                .enableDeleteTempFile(deleteTempFile);

        // naive de-duplication based on ids
        Map<Long, FileContainer> internalIdMap = new HashMap<>();
        Map<String, FileContainer> externalIdMap = new HashMap<>();
        for (FileContainer item : files) {
            if (item.getFileMetadata().hasExternalId()) {
                externalIdMap.put(item.getFileMetadata().getExternalId(), item);
            } else if (item.getFileMetadata().hasId()) {
                internalIdMap.put(item.getFileMetadata().getId(), item);
            } else {
                String message = loggingPrefix + "File item does not contain id nor externalId: " + item.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(loggingPrefix + "Received {} files to upload.", internalIdMap.size() + externalIdMap.size());

        // Combine into list
        List<FileContainer> fileContainerList = new ArrayList<>();
        fileContainerList.addAll(externalIdMap.values());
        fileContainerList.addAll(internalIdMap.values());

        // Results set container
        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();

        // Write files async
        for (FileContainer file : fileContainerList) {
            CompletableFuture<ResponseItems<String>> future = fileWriter.writeFileAsync(
                    addAuthInfo(Request.create()
                            .withProtoRequestBody(file))
            );
            resultFutures.add(future);
        }
        LOG.info(loggingPrefix + "Dispatched {} files for upload. Duration: {}",
                fileContainerList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Failed to upload file to Cognite Data Fusion: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed upload of {} files within a duration of {}.",
                fileContainerList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return responseItems.stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Downloads file binaries.
     *
     * Downloads a set of file binaries based on {@code externalId / id} in the {@link Item} list. The file
     * binaries can be downloaded as files or byte streams. In case the file is very large (> 200MB) it has to
     * be streamed directly to the file system (i.e. downloaded as a file).
     *
     * Both the file header / metadata and the file binary will be returned. The complete information is encapsulated
     * int the {@link FileContainer} returned from this method. The {@link FileContainer} will host the file
     * binary stream if you set {@code preferByteStream} to {@code true} and the file size is < 200 MB. If
     * {@code preferByteStream} is set to {@code false} or the file size is > 200MB the file binary will be
     * stored on disk and the {@link FileContainer} will return the {@link URI} reference to the
     * binary.
     *
     * Supported destination file stores for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * @param files The list of files to download.
     * @param downloadPath The URI to the download storage
     * @param preferByteStream Set to true to return byte streams when possible, set to false to always store
     *                         binary as file.
     * @return File containers with file headers and references/byte streams of the binary.
     */
    public List<FileContainer> download(List<Item> files, Path downloadPath, boolean preferByteStream) throws Exception {
        String loggingPrefix = "download() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(java.nio.file.Files.isDirectory(downloadPath),
                loggingPrefix + "The download path must be a valid directory.");

        Instant startInstant = Instant.now();
        if (files.isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the download request.");
            return Collections.emptyList();
        }
        LOG.info(loggingPrefix + "Received {} items to download.",
                files.size());

        List<List<Item>> batches = Partition.ofSize(files, MAX_DOWNLOAD_BINARY_BATCH_SIZE);
        List<FileContainer> results = new ArrayList<>();
        for (List<Item> batch : batches) {
            // Get the file binaries
            List<FileBinary> fileBinaries = downloadFileBinaries(batch, downloadPath.toUri(), !preferByteStream);
            // Get the file metadata
            List<FileMetadata> fileMetadataList = retrieve(batch);
            // Merge the binary and metadata
            List<FileContainer> tempNameContainers = buildFileContainers(fileBinaries, fileMetadataList);

            // Rename the file from random temp name to file name
            List<FileContainer> resultContainers = new ArrayList<>();
            for (FileContainer container : tempNameContainers) {
                if (container.getFileBinary().getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY_URI
                        && container.hasFileMetadata()) {
                    // Get the target file name. Replace illegal characters with dashes
                    String fileNameBase = container.getFileMetadata().getName()
                            .trim()
                            .replaceAll("[\\/|\\\\|&|\\$]", "-");
                    String fileSuffix = "";
                    if (fileNameBase.lastIndexOf(".") != -1) {
                        // The file name has a suffix. Let's break it out.
                        int splitIndex = fileNameBase.lastIndexOf(".");
                        fileSuffix = fileNameBase.substring(splitIndex);
                        fileNameBase = fileNameBase.substring(0, splitIndex);
                    }
                    Path tempFilePath = Paths.get(new URI(container.getFileBinary().getBinaryUri()));
                    String destinationFileName = fileNameBase;
                    int counter = 1;
                    while (java.nio.file.Files.exists(downloadPath.resolve(destinationFileName + fileSuffix))) {
                        // The destination file name already exists, so we add an increasing counter to the
                        // file name base.
                        destinationFileName = fileNameBase + "_" + counter;
                        counter++;
                    }

                    // Rename the file
                    Path destinationPath = downloadPath.resolve(destinationFileName + fileSuffix);
                    java.nio.file.Files.move(tempFilePath, destinationPath);

                    // Build a new file container with the new file name
                    FileContainer updated = container.toBuilder()
                            .setFileBinary(container.getFileBinary().toBuilder()
                                    .setBinaryUri(destinationPath.toUri().toString()))
                            .build();

                    // Swap the old container with the new one
                    resultContainers.add(updated);
                } else {
                    resultContainers.add(container);
                }
            }
            results.addAll(resultContainers);
        }
        LOG.info(loggingPrefix + "Successfully downloaded {} files within a duration of {}.",
                results.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return results;
    }

    /**
     * Downloads file binaries to a local / network path.
     *
     * Downloads a set of file binaries based on {@code externalId / id} in the {@link Item} list.
     *
     * Both the file header / metadata and the file binary will be returned. The complete information is encapsulated
     * int the {@link FileContainer} returned from this method. The {@link FileContainer} will host the
     * {@link URI} reference to the binary.
     *
     * Supported destination file stores for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * @param files The list of files to download.
     * @param downloadPath The URI to the download storage
     * @return File containers with file headers and references/byte streams of the binary.
     */
    public List<FileContainer> downloadToPath(List<Item> files, Path downloadPath) throws Exception {
        return download(files, downloadPath, false);
    }

    /*
    Gathers file binaries and metadata into file containers via externalId / id.
     */
    private List<FileContainer> buildFileContainers(Collection<FileBinary> inputBinaries,
                                                    Collection<FileMetadata> inputMetadata) {
        List<FileContainer> containers = new ArrayList<>();
        for (FileBinary binary : inputBinaries) {
            FileContainer.Builder containerBuilder = FileContainer.newBuilder()
                    .setFileBinary(binary);
            if (binary.getIdTypeCase() == FileBinary.IdTypeCase.EXTERNAL_ID
                    && getByExternalId(inputMetadata, binary.getExternalId()).isPresent()) {
                containerBuilder.setFileMetadata(getByExternalId(inputMetadata, binary.getExternalId()).get());
            } else if (binary.getIdTypeCase() == FileBinary.IdTypeCase.ID
                    && getById(inputMetadata, binary.getId()).isPresent()) {
                containerBuilder.setFileMetadata(getById(inputMetadata, binary.getId()).get());
            }
            containers.add(containerBuilder.build());
        }
        return containers;
    }

    /**
     * Downloads file binaries.
     *
     * This method is intended for advanced use cases, for example when using this SDK as a part of
     * a distributed system.
     *
     * Downloads a set of file binaries based on {@code externalId / id} in the {@link Item} list. The file
     * binaries can be downloaded as files or byte streams. In case the file is very large (> 200MB) it has to
     * be streamed directly to the file system (to the temp storage area).
     *
     * Supported temp storage for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * @param fileItems The list of files to download.
     * @param tempStoragePath The URI to the download storage. Set to null to only perform in-memory download.
     * @param forceTempStorage Set to true to always download the binary to temp storage
     * @return The file binary.
     * @throws Exception
     */
    public List<FileBinary> downloadFileBinaries(List<Item> fileItems,
                                                 @Nullable URI tempStoragePath,
                                                 boolean forceTempStorage) throws Exception {
        final int MAX_RETRIES = 3;
        String loggingPrefix = "downloadFileBinaries() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(!(null == tempStoragePath && forceTempStorage),
                "Illegal parameter combination. You must specify a URI in order to force temp storage.");
        Preconditions.checkArgument(itemsHaveId(fileItems),
                loggingPrefix + "All file items must include a valid externalId or id.");

        Instant startInstant = Instant.now();
        // do not send empty requests.
        if (fileItems.isEmpty()) {
            LOG.warn(loggingPrefix + "Tried to send empty delete request. Will skip this request.");
            return Collections.emptyList();
        }
        LOG.info(loggingPrefix + "Received request to download {} file binaries.",
                fileItems.size());

        // Download and completed lists
        List<Item> elementListDownload = deDuplicate(fileItems);
        List<FileBinary> elementListCompleted = new ArrayList<>();

        /*
        Responses from readFileBinaryById will be a single item in case of an error. Check that item for success,
        missing items and duplicates.
         */
        // if the request result is false, we have duplicates and/or missing items.
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String exceptionMessage = "";
        for (int i = 0; i < MAX_RETRIES && elementListDownload.size() > 0;
             i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.debug(loggingPrefix + "Start download loop {} with {} items to download and "
                            + "{} completed items at duration {}",
                    i,
                    elementListDownload.size(),
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            /*
            Download files
             */
            Map<List<ResponseItems<FileBinary>>, List<Item>> downloadResponseMap =
                    splitAndDownloadFileBinaries(elementListDownload, tempStoragePath, forceTempStorage);
            LOG.debug(loggingPrefix + "Completed download files requests for {} files across {} batches at duration {}",
                    elementListDownload.size(),
                    downloadResponseMap.size(),
                    Duration.between(startInstant, Instant.now()).toString());
            elementListDownload.clear();

            for (List<ResponseItems<FileBinary>> responseBatch : downloadResponseMap.keySet()) {
                if (responseBatch.size() > 0 && responseBatch.get(0).isSuccessful()) {
                    // All files downloaded successfully
                    for (ResponseItems<FileBinary> response : responseBatch) {
                        if (response.isSuccessful()) {
                            elementListCompleted.addAll(response.getResultsItems());
                        } else {
                            // Should not be possible...
                            LOG.warn(loggingPrefix + "Download not successful: {}", response.getResponseBodyAsString());
                        }
                    }
                } else if (responseBatch.size() > 0 && !responseBatch.get(0).isSuccessful()) {
                    // Batch failed. Most likely because of missing or duplicated items
                    exceptionMessage = responseBatch.get(0).getResponseBodyAsString();
                    LOG.debug(loggingPrefix + "Download items request failed: {}", responseBatch.get(0).getResponseBodyAsString());
                    if (i == MAX_RETRIES - 1) {
                        // Add the error message to std logging
                        LOG.error(loggingPrefix + "Download items request failed. {}", responseBatch.get(0).getResponseBodyAsString());
                    }
                    LOG.debug(loggingPrefix + "Removing duplicates and missing items and retrying the request");
                    List<Item> duplicates = ItemParser.parseItems(responseBatch.get(0).getDuplicateItems());
                    List<Item> missing = new ArrayList(); // Must define this as an explicit List for it to be mutable
                    missing.addAll(ItemParser.parseItems(responseBatch.get(0).getMissingItems()));
                    LOG.debug(loggingPrefix + "No of duplicates reported: {}", duplicates.size());
                    LOG.debug(loggingPrefix + "No of missing items reported: {}", missing.size());

                    // Check for the special case of missing file binaries
                    if (responseBatch.size() > 0 && !responseBatch.get(0).isSuccessful()
                            && responseBatch.get(0).getResponseBinary().getResponse().code() == 400
                            && responseBatch.get(0).getErrorMessage().size() > 0
                            && responseBatch.get(0).getErrorMessage().get(0).startsWith("Files not uploaded,")) {
                        // There is a file binary that hasn't been uploaded, but the file header exists.
                        // Add the items to the "missing" list so they get removed from the download list.
                        LOG.debug(loggingPrefix + "Missing file binaries reported: {}", responseBatch.get(0).getErrorMessage().get(0));
                        if (responseBatch.get(0).getErrorMessage().get(0).startsWith("Files not uploaded, ids:")) {
                            String[] missingIds = responseBatch.get(0).getErrorMessage().get(0).substring(24).split(",");
                            for (String stringId : missingIds) {
                                missing.add(Item.newBuilder()
                                        .setId(Long.parseLong(stringId.trim()))
                                        .build());
                            }
                        } else if (responseBatch.get(0).getErrorMessage().get(0).startsWith("Files not uploaded, externalIds:")) {
                            String[] missingExternalIds = responseBatch.get(0).getErrorMessage().get(0).substring(32).split(",");
                            for (String externalId : missingExternalIds) {
                                missing.add(Item.newBuilder()
                                        .setExternalId(externalId.trim())
                                        .build());
                            }
                        }
                    }

                    // Remove missing items from the download request
                    Map<String, Item> itemsMap = mapItemToId(downloadResponseMap.get(responseBatch));
                    for (Item value : missing) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            itemsMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            itemsMap.remove(String.valueOf(value.getId()));
                        }
                    }

                    // Remove duplicate items from the download request
                    for (Item value : duplicates) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            itemsMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            itemsMap.remove(String.valueOf(value.getId()));
                        }
                    }

                    elementListDownload.addAll(itemsMap.values());
                }
            }
        }

        // Check if all elements completed the download requests
        if (elementListDownload.isEmpty()) {
            LOG.info(loggingPrefix + "Successfully downloaded {} files within a duration of {}.",
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        } else {
            LOG.error(loggingPrefix + "Failed to download files. {} files remaining. {} files completed delete."
                            + System.lineSeparator() + "{}",
                    elementListDownload.size(),
                    elementListCompleted.size(),
                    exceptionMessage);
            throw new Exception(String.format(loggingPrefix + "Failed to download files. %d files remaining. "
                            + " %d files completed download. %n " + exceptionMessage,
                    elementListDownload.size(),
                    elementListCompleted.size()));
        }

        return elementListCompleted;
    }

    /**
     * Deletes a set of files.
     *
     * The files to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * @param files a list of {@link Item} representing the events (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> files) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteFiles();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(files);
    }

    /**
     * Download a set of file binaries. Large batches are split into multiple download requests.
     *
     * @param fileItems The list of files to download.
     * @param tempStoragePath The URI to the download storage. Set to null to only perform in-memory download.
     * @param forceTempStorage Set to true to always download the binary to temp storage
     * @return The file binary response map.
     * @throws Exception
     */
    private Map<List<ResponseItems<FileBinary>>, List<Item>> splitAndDownloadFileBinaries(List<Item> fileItems,
                                                                                          @Nullable URI tempStoragePath,
                                                                                          boolean forceTempStorage) throws Exception {
        String loggingPrefix = "splitAndDownloadFileBinaries() - ";

        Map<List<ResponseItems<FileBinary>>, List<Item>> responseMap = new HashMap<>();
        List<List<Item>> itemBatches = Partition.ofSize(fileItems, MAX_DOWNLOAD_BINARY_BATCH_SIZE);

        // Set up the download service
        ConnectorServiceV1.FileBinaryReader reader = getClient().getConnectorService().readFileBinariesByIds()
                .enableForceTempStorage(forceTempStorage);

        if (null != tempStoragePath) {
            reader = reader.withTempStoragePath(tempStoragePath);
        }

        // Process all batches.
        for (List<Item> batch : itemBatches) {
            Request request = addAuthInfo(Request.create()
                            .withItems(toRequestItems(deDuplicate(batch))));

            try {
                responseMap.put(reader.readFileBinaries(request), batch);
            } catch (CompletionException | FileBinaryRequestExecutor.ClientRequestException e) {
                // First we need to find out the cause / which exception type is thrown
                FileBinaryRequestExecutor.ClientRequestException requestException = null;
                if (e instanceof FileBinaryRequestExecutor.ClientRequestException) {
                    requestException = (FileBinaryRequestExecutor.ClientRequestException) e;
                } else if (e instanceof CompletionException) {
                    // Unwrap the completion exception to see if the underlying cause is a ClientRequestException
                    requestException = ((CompletionException) e).getCause() instanceof FileBinaryRequestExecutor.ClientRequestException ?
                            ((FileBinaryRequestExecutor.ClientRequestException) ((CompletionException) e).getCause()) : null;
                }

                if (null != requestException) {
                    // This exception indicates a malformed download URL--typically an expired URL. This can be caused
                    // by the parallel downloads interfering with each other. Retry with the file items downloaded individually
                    LOG.warn(loggingPrefix + "Error when downloading a batch of file binaries. Will retry each file individually.");
                    for (Item item : batch) {
                        Request singleItemRequest = addAuthInfo(Request.create()
                                .withItems(toRequestItems(List.of(item))));
                        responseMap.put(reader.readFileBinaries(singleItemRequest), List.of(item));
                    }
                }
            }
        }

        return responseMap;
    }

    /**
     * Update file metadata items.
     *
     * Submits a (large) batch of items by splitting it up into multiple, parallel create / insert requests.
     * The response from each request is returned along with the items used as input.
     *
     * @param fileMetadataList the objects to create/insert.
     * @param updateWriter the ItemWriter to use for sending update requests
     * @return a {@link Map} with the responses and request inputs.
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<FileMetadata>> splitAndUpdateFileMetadata(List<FileMetadata> fileMetadataList,
                                                                                ConnectorServiceV1.ItemWriter updateWriter) throws Exception {
        Map<CompletableFuture<ResponseItems<String>>, List<FileMetadata>> responseMap = new HashMap<>();
        List<List<FileMetadata>> batches = Partition.ofSize(fileMetadataList, MAX_WRITE_REQUEST_BATCH_SIZE);

        // Submit all batches
        for (List<FileMetadata> fileBatch : batches) {
            responseMap.put(updateFileMetadata(fileBatch, updateWriter), fileBatch);
        }

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<FileMetadata>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<FileMetadata>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Adds asset ids to existing file metadata objects.
     *
     * Submits a (large) batch of items by splitting it up into multiple, parallel create / insert requests.
     * The response from each request is returned along with the items used as input.
     *
     * @param fileMetadataList the objects to create/insert.
     * @param updateWriter the ItemWriter to use for sending update requests
     * @return a {@link Map} with the responses and request inputs.
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<FileMetadata>> splitAndAddAssets(List<FileMetadata> fileMetadataList,
                                                                                      ConnectorServiceV1.ItemWriter updateWriter) throws Exception {
        Map<CompletableFuture<ResponseItems<String>>, List<FileMetadata>> responseMap = new HashMap<>();
        List<List<FileMetadata>> batches = Partition.ofSize(fileMetadataList, MAX_WRITE_REQUEST_BATCH_SIZE);

        // Submit all batches
        for (List<FileMetadata> fileBatch : batches) {
            responseMap.put(addFileAssets(fileBatch, updateWriter), fileBatch);
        }

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<FileMetadata>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<FileMetadata>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Create /insert items.
     *
     * Submits a (large) batch of items by splitting it up into multiple, parallel create / insert requests.
     * The response from each request is returned along with the items used as input.
     *
     * @param fileMetadataList the objects to create/insert.
     * @param createWriter the ItemWriter to use for sending create requests.
     * @return a {@link Map} with the responses and request inputs.
     * @throws Exception
     */
    private Map<ResponseItems<String>, FileMetadata> splitAndCreateFileMetadata(List<FileMetadata> fileMetadataList,
                                                                               ConnectorServiceV1.ItemWriter createWriter) throws Exception {
        Map<CompletableFuture<ResponseItems<String>>, FileMetadata> responseMap = new HashMap<>();

        // Submit all batches
        for (FileMetadata file : fileMetadataList) {
            responseMap.put(createFileMetadata(file, createWriter), file);
        }

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, FileMetadata> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, FileMetadata> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Post a collection of {@link FileMetadata} create request on a separate thread. The response is wrapped in a
     * {@link CompletableFuture} that is returned immediately to the caller.
     *
     * This method will send the entire input in a single request. It does not
     * split the input into multiple batches. If you have a large batch of {@link FileMetadata} that
     * you would like to split across multiple requests, use the {@code splitAndCreateFileMetadata} method.
     *
     * @param fileMetadata
     * @param fileWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> createFileMetadata(FileMetadata fileMetadata,
                                                                      ConnectorServiceV1.ItemWriter fileWriter) throws Exception {
        String loggingPrefix = "createFileMetadata() - ";
        LOG.debug(loggingPrefix + "Received file metadata item / header to create.");

        // build request object
        Request postSeqBody = addAuthInfo(Request.create()
                .withRequestParameters(toRequestInsertItem(fileMetadata)));

        // post write request
        return fileWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Post a collection of {@link FileMetadata} update request on a separate thread. The response is wrapped in a
     * {@link CompletableFuture} that is returned immediately to the caller.
     *
     * This method will send the entire input in a single request. It does not
     * split the input into multiple batches. If you have a large batch of {@link FileMetadata} that
     * you would like to split across multiple requests, use the {@code splitAndUpdateFileMetadata} method.
     *
     * @param filesBatch
     * @param fileWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> updateFileMetadata(Collection<FileMetadata> filesBatch,
                                                                        ConnectorServiceV1.ItemWriter fileWriter) throws Exception {
        String loggingPrefix = "updateFileMetadata() - ";
        LOG.debug(loggingPrefix + "Received {} file metadata items / headers to update.",
                filesBatch.size());
        ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
        for (FileMetadata fileMetadata : filesBatch) {
            if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
                insertItemsBuilder.add(toRequestReplaceItem(fileMetadata));
            } else {
                insertItemsBuilder.add(toRequestUpdateItem(fileMetadata));
            }
        }

        // build request object
        Request postSeqBody = addAuthInfo(Request.create()
                .withItems(insertItemsBuilder.build()));

        // post write request
        return fileWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Patches (adds) a set of assets to a file object. This operation is used when we need to
     * handle files with more than 1k assets.
     *
     * This method will send the entire input in a single request. It does not
     * split the input into multiple batches. If you have a large batch of {@link FileMetadata} that
     * you would like to split across multiple requests, use the {@code splitAndUpdateFileMetadata} method.
     *
     * @param filesBatch
     * @param fileWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> addFileAssets(Collection<FileMetadata> filesBatch,
                                                                        ConnectorServiceV1.ItemWriter fileWriter) throws Exception {
        String loggingPrefix = "patchFileAssets() - ";
        LOG.debug(loggingPrefix + "Received {} file metadata items / headers to update.",
                filesBatch.size());
        ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
        for (FileMetadata fileMetadata : filesBatch) {
            insertItemsBuilder.add(toRequestAddAssetsItem(fileMetadata));
        }

        // build request object
        Request postSeqBody = addAuthInfo(Request.create()
                .withItems(insertItemsBuilder.build()));

        // post write request
        return fileWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Maps the file metadata items to their id by looking up externalId and id.
     *
     * @param fileMetadataList
     * @return
     */
    private Map<String, FileMetadata> mapToId(List<FileMetadata> fileMetadataList) {
        Map<String, FileMetadata> idMap = new HashMap<>();
        for (FileMetadata fileMetadata : fileMetadataList) {
            if (fileMetadata.hasExternalId()) {
                idMap.put(fileMetadata.getExternalId(), fileMetadata);
            } else if (fileMetadata.hasId()) {
                idMap.put(String.valueOf(fileMetadata.getId()), fileMetadata);
            } else {
                idMap.put("", fileMetadata);
            }
        }
        return idMap;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private FileMetadata parseFileMetadata(String json) {
        try {
            return FileParser.parseFileMetadata(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(FileMetadata item) {
        try {
            return FileParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(FileMetadata item) {
        try {
            return FileParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(FileMetadata item) {
        try {
            return FileParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestAddAssetsItem(FileMetadata item) {
        try {
            return FileParser.toRequestAddAssetIdsItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of file metadata. It will first check for an externalId, second it will check for id.
    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getFileId(FileMetadata item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    /*
    Returns the id of file metadata. It will first check for an externalId, second it will check for id.
    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getFileId(FileBinary item) {
        if (item.getIdTypeCase() == FileBinary.IdTypeCase.EXTERNAL_ID) {
            return Optional.of(item.getExternalId());
        } else if (item.getIdTypeCase() == FileBinary.IdTypeCase.ID) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    /*
    Returns the file metadata that matches a given externalId
     */
    private Optional<FileMetadata> getByExternalId(Collection<FileMetadata> itemsToSearch, String externalId) {
        Optional<FileMetadata> returnObject = Optional.empty();
        for (FileMetadata item : itemsToSearch) {
            if (item.getExternalId().equals(externalId)) {
                return Optional.of(item);
            }
        }
        return returnObject;
    }

    /*
    Returns the file metadata that matches a given id
     */
    private Optional<FileMetadata> getById(Collection<FileMetadata> itemsToSearch, long id) {
        Optional<FileMetadata> returnObject = Optional.empty();
        for (FileMetadata item : itemsToSearch) {
            if (item.getId() == id) {
                return Optional.of(item);
            }
        }
        return returnObject;
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Files build();
    }
}
