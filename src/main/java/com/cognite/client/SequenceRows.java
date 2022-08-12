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
import com.cognite.client.dto.*;
import com.cognite.client.queue.UploadQueue;
import com.cognite.client.queue.UpsertTarget;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.cognite.client.util.Items;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Value;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.cognite.client.servicesV1.ConnectorConstants.*;

/**
 * This class represents the Cognite sequence body/rows api endpoint.
 *
 * It provides methods for reading and writing {@link SequenceBody}.
 */
@AutoValue
public abstract class SequenceRows extends ApiBase implements UpsertTarget<SequenceBody, SequenceBody> {
    private static final SequenceMetadata DEFAULT_SEQ_METADATA = SequenceMetadata.newBuilder()
            .setExternalId("SDK_default")
            .setName("SDK_default")
            .setDescription("Default Sequence metadata created by the Java SDK.")
            .build();

    private static Builder builder() {
        return new AutoValue_SequenceRows.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(SequenceRows.class);

    /**
     * Construct a new {@link SequenceRows} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static SequenceRows of(CogniteClient client) {
        return SequenceRows.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link SequenceBody} objects (i.e. sequences rows x columns) that matches the
     * specification set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The sequence bodies are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceBody> listResults = new ArrayList<>();
     *      client
     *          .sequences()
     *          .rows()
     *          .retrieve(Request.create.withRootParameter("id", 1L))
     *          .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param requestParameters the filters to use for retrieving sequences bodies.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> retrieve(Request requestParameters) throws Exception {
        return this.retrieve(ImmutableList.of(requestParameters));
    }

    /**
     * Returns all {@link SequenceBody} objects (i.e. sequences rows x columns) that matches the
     * specification set in the collection of {@link Request}.
     *
     * By submitting a collection of {@link Request}, the requests will be submitted in parallel to
     * Cognite Data Fusion, potentially increasing the overall I/O performance.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The sequence bodies are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceBody> listResults = new ArrayList<>();
     *      client
     *          .sequences()
     *          .rows()
     *          .retrieve(List.of(Request.create.withRootParameter("id", 1L)))
     *          .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param requestParametersList the filters to use for retrieving sequences bodies.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> retrieve(List<Request> requestParametersList) throws Exception {
        // Build the api iterators.
        List<Iterator<CompletableFuture<ResponseItems<String>>>> iterators = new ArrayList<>();
        for (Request requestParameters : requestParametersList) {
            iterators.add(getListResponseIterator(ResourceType.SEQUENCE_BODY, addAuthInfo(requestParameters)));
        }

        // The iterator that will collect results across multiple results streams
        FanOutIterator fanOutIterator = FanOutIterator.of(iterators);

        // Add results object parsing
        return AdapterIterator.of(fanOutIterator, this::parseSequenceBody);
    }

    /**
     * Retrieves {@link SequenceBody} by {@code externalId}.
     * Refer to {@link #retrieveComplete(List)} for more information.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceBody> listResults = new ArrayList<>();
     *      client
     *          .sequences()
     *          .rows()
     *          .retrieveComplete("1", "2")
     *          .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #retrieveComplete(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved sequence bodies.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> retrieveComplete(String... externalId) throws Exception {
        return retrieveComplete(Items.parseItems(externalId));
    }

    /**
     * Retrieves {@link SequenceBody} by {@code internal id}.
     * Refer to {@link #retrieveComplete(List)} for more information.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceBody> listResults = new ArrayList<>();
     *      client
     *          .sequences()
     *          .rows()
     *          .retrieveComplete(1, 2)
     *          .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #retrieveComplete(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved sequence bodies.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> retrieveComplete(long... id) throws Exception {
        return retrieveComplete(Items.parseItems(id));
    }

    /**
     * Retrieves {@link SequenceBody} by {@code externalId / id}.
     *
     * The entire Sequence body (i.e. all rows and columns) will be retrieved.
     *
     * The sequence bodies are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<SequenceBody> retrievedSequenceBody = client.sequences().rows().retrieve(items);
     * }
     * </pre>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param items The sequences {@code externalId / id} to retrieve rows for.
     * @return The retrieved sequence rows / bodies.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> retrieveComplete(List<Item> items) throws Exception {
        List<Request> requestParametersList = new ArrayList<>(items.size());

        // Build the request objects representing the items
        for (Item item : items) {
            Request requestParameters = Request.create()
                    .withRootParameter("limit", DEFAULT_MAX_BATCH_SIZE_SEQUENCES_ROWS);
            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                requestParameters = requestParameters.withRootParameter("externalId", item.getExternalId());
            } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
                requestParameters = requestParameters.withRootParameter("id", item.getId());
            } else {
                throw new Exception("Item does not contain externalId or id: " + item.toString());
            }

            requestParametersList.add(requestParameters);
        }

        return this.retrieve(requestParametersList);
    }

    /**
     * Creates or updates a set of {@link SequenceBody} objects.
     *
     * A {@link SequenceBody} carries the data cells (columns x rows) to be upserted to a sequence. If the
     * main sequence object hasn't been created in Cognite Data Fusion yet (maybe because of a large job where
     * both sequence headers and bodies are upserted in parallel), this method will create the sequence objects
     * based on the information carried in the {@link SequenceBody}.
     *
     * The algorithm runs as follows:
     * 1. Write all {@link SequenceBody} objects to the Cognite API.
     * 2. If one (or more) of the objects fail, check if it is because of missing sequence objects--create temp headers.
     * 3. Retry the failed {@link SequenceBody} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceBody> sequenceBodies = // List of SequenceBody;
     *      client.sequences().rows().upsert(sequenceBodies);
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param sequenceBodies The sequences rows to upsert
     * @return The upserted sequences rows
     * @throws Exception
     */
    public List<SequenceBody> upsert(List<SequenceBody> sequenceBodies) throws Exception {
        Instant startInstant = Instant.now();
        String batchLogPrefix =
                "upsert() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(sequenceBodies.stream().allMatch(sequenceBody -> getSequenceId(sequenceBody).isPresent()),
                batchLogPrefix + "All items must have externalId or id.");

        int inputRowsCounter = 0;
        int inputCellsCounter = 0;
        for (SequenceBody sequenceBody : sequenceBodies) {
            inputRowsCounter += sequenceBody.getRowsCount();
            inputCellsCounter += sequenceBody.getRowsCount() * sequenceBody.getColumnsCount();
        }
        LOG.debug(batchLogPrefix + "Received {} sequence body objects with {} cells across {} rows to upsert",
                sequenceBodies.size(),
                inputCellsCounter,
                inputRowsCounter);

        // Should not happen--but need to guard against empty input
        if (sequenceBodies.isEmpty()) {
            LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
            return Collections.<SequenceBody>emptyList();
        }

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeSequencesRows();

        /*
        Start the upsert:
        1. Write all sequences to the Cognite API.
        2. If one (or more) of the sequences fail, it is most likely because of missing headers or missing columns.
            Add temp headers or extra columns.
        3. Retry the failed sequences
        */
        Map<ResponseItems<String>, List<SequenceBody>> responseMap = splitAndUpsertSeqBody(sequenceBodies, createItemWriter);

        // Check for unsuccessful request
        List<Item> missingItems = new ArrayList<>();
        Map<List<SequenceBody>, String> missingColumns = new HashMap<>();
        List<SequenceBody> retrySequenceBodyList = new ArrayList<>();
        List<ResponseItems<String>> successfulBatches = new ArrayList<>();
        boolean requestsAreSuccessful = true;
        String errorMessage = "";
        for (ResponseItems<String> responseItems : responseMap.keySet()) {
            requestsAreSuccessful = requestsAreSuccessful && responseItems.isSuccessful();
            if (!responseItems.isSuccessful()) {
                // Check for duplicates. Duplicates should not happen, so fire off an exception.
                if (!responseItems.getDuplicateItems().isEmpty()) {
                    String message = String.format(batchLogPrefix + "Duplicates reported: %d %n "
                                    + "Response body: %s",
                            responseItems.getDuplicateItems().size(),
                            responseItems.getResponseBodyAsString()
                                    .substring(0, Math.min(1000, responseItems.getResponseBodyAsString().length())));
                    LOG.error(message);
                    throw new Exception(message);
                }

                // Add the original sequence bodies to the retry list
                retrySequenceBodyList.addAll(responseMap.get(responseItems));

                // Get the missing items and add the original sequence bodies to the retry list
                missingItems.addAll(parseItems(responseItems.getMissingItems()));

                // Check if the cause is missing columns
                if (responseItems.getStatus().size() > 0
                        && responseItems.getStatus().get(0).equalsIgnoreCase("404")
                        && responseItems.getErrorMessage().size() > 0
                        && responseItems.getErrorMessage().get(0).startsWith("Can't find column")) {
                    missingColumns.put(responseMap.get(responseItems), responseItems.getErrorMessage().get(0));
                }

                // Add the most recent error response so we can report on it later
                errorMessage = responseItems.getResponseBodyAsString();
            } else {
                successfulBatches.add(responseItems);
            }
        }

        if (!requestsAreSuccessful) {
            LOG.warn(batchLogPrefix + "Write sequence rows failed. Most likely due to missing sequence header / metadata"
                    + " or missing columns. Will add minimum sequence metadata or the extra column(s)"
                    + " and retry the sequence rows insert.");
            LOG.info(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());
            LOG.info(batchLogPrefix + "Number of missing columns reported by CDF: {}", missingColumns.size());

            /*
            Handle missing sequence headers. Since the CDF API is nice and reports all missing headers back to us,
            we can iterate through them and add a minimum header for all sequences in question before retrying the
            rows upsert.
             */
            // check if the missing items are based on internal id--not supported
            List<SequenceBody> missingSequences = new ArrayList<>(missingItems.size());
            for (Item item : missingItems) {
                if (item.getIdTypeCase() != Item.IdTypeCase.EXTERNAL_ID) {
                    String message = batchLogPrefix + "Sequence with internal id refers to a non-existing sequence. "
                            + "Only externalId is supported. Item specification: " + item.toString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                // add the corresponding sequence body to a list for later processing
                sequenceBodies.stream()
                        .filter(sequence -> sequence.getExternalId().equals(item.getExternalId()))
                        .forEach(missingSequences::add);
            }
            LOG.debug(batchLogPrefix + "All missing items are based on externalId");

            // If we have missing items, add default sequence header
            if (missingSequences.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write sequences rows failed, but cannot identify missing sequences headers");
            } else {
                LOG.debug(batchLogPrefix + "Start writing default sequence headers for {} items",
                        missingSequences.size());
                writeSeqHeaderForRows(missingSequences);
                LOG.debug(batchLogPrefix + "Finished writing default headers.",
                        retrySequenceBodyList.size());
            }

            /*
            Hande missing columns. CDF does not report all missing columns--just the first occurence. Therefore we cannot
            guarantee to handle all missing columns.
             */
            for (Map.Entry<List<SequenceBody>, String> entry : missingColumns.entrySet()) {
                addSequenceColumnsForRows(entry.getKey(), entry.getValue());
            }

            // Retry the failed sequence body upsert
            if (retrySequenceBodyList.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write sequences rows failed, but cannot identify sequences to retry.");
            } else {
                LOG.debug(batchLogPrefix + "Will retry {} sequence body items.",
                        retrySequenceBodyList.size());
                Map<ResponseItems<String>, List<SequenceBody>> retryResponseMap =
                        splitAndUpsertSeqBody(retrySequenceBodyList, createItemWriter);

                // Check status of the requests
                requestsAreSuccessful = true;
                for (ResponseItems<String> responseItems : retryResponseMap.keySet()) {
                    requestsAreSuccessful = requestsAreSuccessful && responseItems.isSuccessful();
                    if (!responseItems.isSuccessful()) {
                        // Add the most recent error response so we can report on it later
                        errorMessage = responseItems.getResponseBodyAsString();
                    }
                }
            }
        }

        if (!requestsAreSuccessful) {
            String message = String.format(batchLogPrefix + "Failed to write sequences rows: %n%s", errorMessage);
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.info(batchLogPrefix + "Completed writing {} sequence items with {} total rows and {} cells "
                        + "across {} requests within a duration of {}.",
                sequenceBodies.size(),
                inputRowsCounter,
                inputCellsCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return ImmutableList.copyOf(sequenceBodies);
    }

    /**
     * Returns an upload queue.
     *
     * The upload queue helps improve performance by batching items together before uploading them to Cognite Data Fusion.
     *
     * {@link SequenceBody} objects are quite large, so the queue is tuned with a default size of 10.
     * @return The upload queue.
     */
    public UploadQueue<SequenceBody, SequenceBody> uploadQueue() {
        return UploadQueue.of(this)
                .withQueueSize(10);
    }

    /**
     * Deletes the given rows of the sequence(s).
     *
     * This method will delete the rows specified via the sequence externalId/id + row number list in the input
     * {@link SequenceBody} objects. You don't need to specify columns or values. All columns will always be removed
     * from the listed row numbers.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<SequenceBody> sequenceRows = // List of SequenceBody;
     *     List<SequenceBody> deletedItemsResults = client.sequences().rows().delete(sequenceRows);
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#sequences()
     * @see Sequences#rows()
     *
     * @param sequenceRows
     * @return The deleted rows
     * @throws Exception
     */
    public List<SequenceBody> delete(List<SequenceBody> sequenceRows) throws Exception {
        String loggingPrefix = "delete() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        int maxDeleteLoopIterations = 3;

        // should not happen, but need to guard against empty input
        if (sequenceRows.isEmpty()) {
            LOG.warn(loggingPrefix + "No items in the input. Returning without deleting any rows.");
            return Collections.emptyList();
        }

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteSequencesRows();

        // Delete and completed lists
        List<SequenceBody> elementListDelete = sequenceRows;
        List<SequenceBody> elementListCompleted = new ArrayList<>(elementListDelete.size());

        /*
        The delete loop. If there are items left to delete:
        1. Delete items
        2. If conflict, remove duplicates and missing items.
        */
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String exceptionMessage = "";
        for (int i = 0; i < maxDeleteLoopIterations && elementListDelete.size() > 0;
             i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.debug(loggingPrefix + "Start delete loop {} with {} sequence body items to delete and "
                            + "{} completed items at duration {}",
                    i,
                    elementListDelete.size(),
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            /*
            Delete items
             */
            Map<ResponseItems<String>, List<SequenceBody>> deleteResponseMap =
                    splitAndDeleteItems(elementListDelete, deleteItemWriter);
            LOG.debug(loggingPrefix + "Completed delete items requests for {} items across {} batches at duration {}",
                    elementListDelete.size(),
                    deleteResponseMap.size(),
                    Duration.between(startInstant, Instant.now()).toString());
            elementListDelete.clear(); // Must prepare the list for possible new entries.

            for (ResponseItems<String> response : deleteResponseMap.keySet()) {
                if (response.isSuccessful()) {
                    elementListCompleted.addAll(deleteResponseMap.get(response));
                    LOG.debug(loggingPrefix + "Delete items request success. Adding {} delete result items to result collection.",
                            deleteResponseMap.get(response).size());
                } else {
                    exceptionMessage = response.getResponseBodyAsString();
                    LOG.debug(loggingPrefix + "Delete items request failed: {}", response.getResponseBodyAsString());
                    if (i == maxDeleteLoopIterations - 1) {
                        // Add the error message to std logging
                        LOG.error(loggingPrefix + "Delete items request failed. {}", response.getResponseBodyAsString());
                    }
                    LOG.debug(loggingPrefix + "Delete items request failed. "
                            + "Removing duplicates and missing items and retrying the request");
                    List<Item> duplicates = ItemParser.parseItems(response.getDuplicateItems());
                    List<Item> missing = ItemParser.parseItems(response.getMissingItems());
                    LOG.debug(loggingPrefix + "No of duplicate entries reported by CDF: {}", duplicates.size());
                    LOG.debug(loggingPrefix + "No of missing items reported by CDF: {}", missing.size());

                    // Remove missing items from the delete request
                    Map<String, SequenceBody> itemsMap = mapToId(deleteResponseMap.get(response));
                    for (Item value : missing) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            itemsMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            itemsMap.remove(String.valueOf(value.getId()));
                        }
                    }

                    // Remove duplicate items from the delete request
                    for (Item value : duplicates) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            itemsMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            itemsMap.remove(String.valueOf(value.getId()));
                        }
                    }

                    elementListDelete.addAll(itemsMap.values()); // Add remaining items to be re-deleted
                }
            }
        }

        // Check if all elements completed the upsert requests
        if (elementListDelete.isEmpty()) {
            LOG.info(loggingPrefix + "Successfully deleted {} items within a duration of {}.",
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        } else {
            LOG.error(loggingPrefix + "Failed to delete items. {} items remaining. {} items completed delete."
                            + System.lineSeparator() + "{}",
                    elementListDelete.size(),
                    elementListCompleted.size(),
                    exceptionMessage);
            throw new Exception(String.format(loggingPrefix + "Failed to upsert items. %d items remaining. "
                            + " %d items completed upsert. %n " + exceptionMessage,
                    elementListDelete.size(),
                    elementListCompleted.size()));
        }

        return elementListCompleted;
    }

    /**
     * Delete sequences rows.
     *
     * Submits a (large) batch of sequence body / row items by splitting it up into multiple, parallel delete requests.
     * The response from each request is returned along with the items used as input.
     *
     * This method will:
     * 1. Check all input for valid ids.
     * 2. Deduplicate the items, including row numbers.
     * 3. Split the input into request batches (if necessary).
     *
     * @param sequenceRows The sequence rows to delete
     * @param deleteWriter The {@link com.cognite.client.servicesV1.ConnectorServiceV1.ItemWriter} to use for the delete requests.
     * @return A {@link Map} with the responses and request inputs.
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<SequenceBody>> splitAndDeleteItems(List<SequenceBody> sequenceRows,
                                                                               ConnectorServiceV1.ItemWriter deleteWriter) throws Exception {
        String loggingPrefix = "splitAndDeleteItems() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        int maxItemsPerBatch = 1000;
        int maxRowsPerItem = 10_000;
        List<SequenceBody> deleteItemsList = new ArrayList<>(sequenceRows.size());

        /*
         check that ids are provided + remove duplicate rows.
         1. Map all input objects to id.
         2. Consolidate all rows per id.
         */
        Map<String, List<SequenceBody>> itemMap = new HashMap<>();
        long rowCounter = 0;
        for (SequenceBody value : sequenceRows) {
            if (getSequenceId(value).isPresent()) {
                List<SequenceBody> rows = itemMap.getOrDefault(getSequenceId(value).get(), new ArrayList<>());
                rows.add(value);
                itemMap.put(getSequenceId(value).get(), rows);
                rowCounter += value.getRowsCount();
            } else {
                String message = loggingPrefix + "Sequence does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.debug(loggingPrefix + "Received {} rows to remove from {} sequences, with {} unique sequence ids. Duration of: {}",
                rowCounter,
                sequenceRows.size(),
                itemMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        int dedupeRowCounter = 0;
        for (List<SequenceBody> elements : itemMap.values()) {
            List<Long> sequenceRowNumbers = new ArrayList<>();
            SequenceBody sequenceBody = elements.get(0).toBuilder()
                    .clearRows()
                    .clearColumns()
                    .build();
            for (SequenceBody item : elements) {
                // deduplicate row numbers
                Set<Long> uniqueRowNos = new HashSet<>(sequenceRowNumbers.size() + item.getRowsCount());
                uniqueRowNos.addAll(sequenceRowNumbers);
                item.getRowsList().forEach(row -> uniqueRowNos.add(row.getRowNumber()));
                sequenceRowNumbers = new ArrayList<>(uniqueRowNos);
            }
            List<SequenceRow> sequenceRowsDeduplicated = new ArrayList<>();
            sequenceRowNumbers.forEach(rowNumber ->
                    sequenceRowsDeduplicated.add(SequenceRow.newBuilder().setRowNumber(rowNumber).build()));
            deleteItemsList.add(sequenceBody.toBuilder()
                    .addAllRows(sequenceRowsDeduplicated)
                    .build());
            dedupeRowCounter += sequenceRowsDeduplicated.size();
        }
        LOG.debug(loggingPrefix + "Finished deduplication. Result: {} rows across {} sequences. Duration of: {}",
                dedupeRowCounter,
                deleteItemsList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Split into batches and submit delete requests
        Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap = new HashMap<>();
        List<SequenceBody> deleteBatch = new ArrayList<>(maxItemsPerBatch);
        int submitDeleteRowCounter = 0;
        int submitItemsCounter = 0;
        for (SequenceBody deleteItem : deleteItemsList) {
            // Check if there are too many rows per item
            if (deleteItem.getRowsCount() > maxRowsPerItem) {
                List<List<SequenceRow>> rowBatches = Partition.ofSize(deleteItem.getRowsList(), maxRowsPerItem);
                for (List<SequenceRow> rowBatch : rowBatches) {
                    deleteBatch.add(deleteItem.toBuilder()
                            .clearRows()
                            .addAllRows(rowBatch)
                            .build());
                    submitDeleteRowCounter += rowBatch.size();

                    // Always submit a request when splitting up the rows for a single sequence id.
                    // Because we cannot have multiple items with the same id in the same batch.
                    responseMap.put(deleteItems(deleteBatch, deleteWriter), deleteBatch);
                    submitItemsCounter += deleteBatch.size();
                    deleteBatch = new ArrayList<>();
                }
            } else {
                deleteBatch.add(deleteItem);
                submitDeleteRowCounter += deleteItem.getRowsCount();
                if (deleteBatch.size() >= maxItemsPerBatch) {
                    responseMap.put(deleteItems(deleteBatch, deleteWriter), deleteBatch);
                    submitItemsCounter += deleteBatch.size();
                    deleteBatch = new ArrayList<>();
                }
            }
        }
        if (!deleteBatch.isEmpty()) {
            responseMap.put(deleteItems(deleteBatch, deleteWriter), deleteBatch);
            submitItemsCounter += deleteBatch.size();
        }
        LOG.debug(loggingPrefix + "Finished submitting delete requests for {} rows across {} sequences items via {} batches. "
                        + "Duration of: {}",
                submitDeleteRowCounter,
                submitItemsCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(futureList::add);
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<SequenceBody>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Submits a set of items as a delete sequence rows request to the Cognite API.
     *
     * @param sequenceRows the objects to delete.
     * @return a {@link CompletableFuture} representing the response from the create request.
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> deleteItems(List<SequenceBody> sequenceRows,
                                                                 ConnectorServiceV1.ItemWriter deleteWriter) throws Exception {
        ImmutableList.Builder<Map<String, Object>> deleteItemsBuilder = ImmutableList.builder();
        // Build the delete items request objects
        for (SequenceBody sequenceBody : sequenceRows) {
            Map<String, Object> deleteItemObject = SequenceParser.toRequestDeleteRowsItem(sequenceBody);
            deleteItemsBuilder.add(deleteItemObject);
        }

        // build request object
        Request deleteItemsRequest = addAuthInfo(Request.create()
                .withItems(deleteItemsBuilder.build()));

        // post write request
        return deleteWriter.writeItemsAsync(deleteItemsRequest);
    }

    /**
     * Writes a (large) batch of {@link SequenceBody} by splitting it up into multiple parallel requests.
     *
     * The response from each individual request is returned along with its part of the input.
     *
     * @param itemList
     * @param seqBodyCreateWriter
     * @return
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<SequenceBody>> splitAndUpsertSeqBody(List<SequenceBody> itemList,
                                                                                 ConnectorServiceV1.ItemWriter seqBodyCreateWriter) throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "splitAndUpsertSeqBody() - ";
        Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap = new HashMap<>();
        List<SequenceBody> batch = new ArrayList<>();
        List<String> sequenceIds = new ArrayList<>(); // To check for existing / duplicate item ids
        int totalItemCounter = 0;
        int totalRowCounter = 0;
        int totalCellsCounter = 0;
        int totalCharacterCounter = 0;
        int batchCellsCounter = 0;
        int batchCharacterCounter = 0;
        for (SequenceBody sequence : itemList)  {
            // Check for duplicate items in the same batch
            if (getSequenceId(sequence).isPresent()) {
                if (sequenceIds.contains(getSequenceId(sequence).get())) {
                    // The externalId / id already exists in the batch, submit it
                    responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                    batch = new ArrayList<>();
                    batchCellsCounter = 0;
                    batchCharacterCounter = 0;
                    sequenceIds.clear();
                }
                sequenceIds.add(getSequenceId(sequence).get());
            }

            List<SequenceRow> rowList = new ArrayList<>();
            for (SequenceRow row : sequence.getRowsList()) {
                // Check if the new row will make the current batch too large.
                // If yes, add the current row list to the batch and submit it before adding the new row
                if ((batchCellsCounter + getColumnsCount(row)) >= DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH
                        || (batchCharacterCounter + getCharacterCount(row)) >= DEFAULT_SEQUENCE_WRITE_MAX_CHARS_PER_BATCH) {
                    if (!rowList.isEmpty()) {
                        // Add the current rows as a new item together with the sequence reference
                        batch.add(sequence.toBuilder()
                                .clearRows()
                                .addAllRows(rowList)
                                .build());
                        rowList = new ArrayList<>();
                        totalItemCounter++;
                    }

                    // Submit the batch
                    responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                    batch = new ArrayList<>();
                    batchCellsCounter = 0;
                    batchCharacterCounter = 0;
                    sequenceIds.clear();
                }

                // Add the row to the row list
                rowList.add(row);
                batchCellsCounter += getColumnsCount(row);
                totalCellsCounter += getColumnsCount(row);
                batchCharacterCounter += getCharacterCount(row);
                totalCharacterCounter += getCharacterCount(row);
                totalRowCounter++;
            }
            if (rowList.size() > 0) {
                batch.add(sequence.toBuilder()
                        .clearRows()
                        .addAllRows(rowList)
                        .build());
                totalItemCounter++;
                if (getSequenceId(sequence).isPresent()) {
                    // Must add id here as well to cover for a corner case where a sequences body has been
                    // "partitioned" by submitting some parts in earlier batches. Then we must make sure that
                    // the id is captured for duplicate detection.
                    sequenceIds.add(getSequenceId(sequence).get());
                }
            }

            if (batch.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH) {
                responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                batch = new ArrayList<>();
                batchCellsCounter = 0;
                batchCharacterCounter = 0;
                sequenceIds.clear();
            }
        }
        if (batch.size() > 0) {
            responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
        }

        LOG.debug(loggingPrefix + "Finished submitting {} cells with {} characters by {} rows across {} sequence items "
                        + "in {} requests batches. Duration: {}",
                totalCellsCounter,
                totalCharacterCounter,
                totalRowCounter,
                totalItemCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()));

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(futureList::add);
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<SequenceBody>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Post a {@link SequenceBody} upsert request on a separate thread. The response is wrapped in a
     * {@link CompletableFuture} that is returned immediately to the caller.
     *
     * This method will send the entire input in a single request. It does not
     * split the input into multiple batches. If you have a large batch of {@link SequenceBody} that
     * you would like to split across multiple requests, use the {@code splitAndUpsertSeqBody} method.
     *
     * @param itemList
     * @param seqBodyCreateWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> upsertSeqBody(List<SequenceBody> itemList,
                                                                   ConnectorServiceV1.ItemWriter seqBodyCreateWriter) throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "upsertSeqBody() - ";
        // Check that all sequences carry an id/externalId + no duplicates + build items list
        ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
        int rowCounter = 0;
        int maxColumnCounter = 0;
        int cellCounter = 0;
        int characterCounter = 0;
        List<String> sequenceIds = new ArrayList<>();
        for (SequenceBody item : itemList) {
            rowCounter += item.getRowsCount();
            maxColumnCounter = Math.max(maxColumnCounter, item.getColumnsCount());
            cellCounter += (item.getColumnsCount() * item.getRowsCount());
            characterCounter += getCharacterCount(item);
            if (!(item.hasExternalId() || item.hasId())) {
                throw new Exception(loggingPrefix + "Sequence body does not contain externalId nor id");
            }
            if (getSequenceId(item).isPresent()) {
                if (sequenceIds.contains(getSequenceId(item).get())) {
                    throw new Exception(String.format(loggingPrefix + "Duplicate sequence body items detected. ExternalId: %s",
                            getSequenceId(item).get()));
                }
                sequenceIds.add(getSequenceId(item).get());
            }
            insertItemsBuilder.add(SequenceParser.toRequestInsertItem(item));
        }

        LOG.debug(loggingPrefix + "Starting the upsert sequence body request. "
                        + "No sequences: {}, Max no columns: {}, Total no rows: {}, Total no cells: {}. "
                        + "Total no characters: {}. Duration: {} ",
                itemList.size(),
                maxColumnCounter,
                rowCounter,
                cellCounter,
                characterCounter,
                Duration.between(startInstant, Instant.now()).toString());

        // build request object
        Request postSeqBody = addAuthInfo(Request.create()
                .withItems(insertItemsBuilder.build()));

        // post write request
        return seqBodyCreateWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Inserts default sequence headers for the input sequence list.
     */
    private void writeSeqHeaderForRows(List<SequenceBody> sequenceList) throws Exception {
        List<SequenceMetadata> sequenceMetadataList = new ArrayList<>(sequenceList.size());
        sequenceList.forEach(sequenceBody -> sequenceMetadataList.add(generateDefaultSequenceMetadataInsertItem(sequenceBody)));

        if (!sequenceMetadataList.isEmpty()) {
            getClient().sequences().upsert(sequenceMetadataList);
        }
    }

    /**
     * Builds a single sequence header with default values. It relies on information completeness
     * related to the columns as these cannot be updated at a later time.
     */
    private SequenceMetadata generateDefaultSequenceMetadataInsertItem(SequenceBody body) {
        Preconditions.checkArgument(body.hasExternalId(),
                "Sequence body is not based on externalId: " + body.toString());

        return DEFAULT_SEQ_METADATA.toBuilder()
                .setExternalId(body.getExternalId())
                .addAllColumns(body.getColumnsList())
                .build();
    }

    /**
     * Adds column(s) to sequence headers for the input sequence body list.
     * 1. Identify the missing column external id. Verify that it is a missing column case
     * 2. Retrieve the relevant current sequence headers from CDF
     * 3. Compare the CDF sequence headers with the sequence bodies column definitions
     * 4. Update the sequence headers with additional column definitions
     *
     * @param sequenceList
     * @param missingColumns
     * @throws Exception
     */
    private void addSequenceColumnsForRows(List<SequenceBody> sequenceList, String missingColumns) throws Exception {
        String loggingPrefix = "addSequenceColumnsForRows() - ";

        // Identify the missing column externalId
        Pattern regEx = Pattern.compile("^Can't find column '(\\S+)'");
        Matcher matcher = regEx.matcher(missingColumns);
        if (matcher.find()) {
            String columnExternalId = matcher.group(1);
            LOG.debug(loggingPrefix + "Identified missing column externalId: {}", columnExternalId);

            Map<String, SequenceBody> sequenceBodyMap = mapToId(sequenceList);
            String[] seqExtIds = new String[sequenceBodyMap.keySet().size()];
            sequenceBodyMap.keySet().toArray(seqExtIds);
            LOG.debug(loggingPrefix + "Checking {} sequence headers for column schema", seqExtIds.length);

            // Retrieve the current sequence headers in CDF
            List<SequenceMetadata> sequenceHeaderCdf = getClient()
                    .sequences().retrieve(seqExtIds);

            // Compare column schemas to identify new columns
            List<SequenceMetadata> sequenceHeaderToUpdate = new ArrayList<>();
            for (Map.Entry<String, SequenceBody> entry : sequenceBodyMap.entrySet()) {
                // Find the CDF sequence header matching the current sequence body
                SequenceMetadata currentHeader = sequenceHeaderCdf.stream()
                        .filter(rows -> rows.getExternalId().equals(entry.getKey()))
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException(String.format(loggingPrefix
                                + "Missing sequence column extId %s for sequence extId %s. Not able to update column "
                                + "schema because the sequence header cannot be retrieved from CDF",
                                columnExternalId,
                                entry.getKey())));

                // Check if the sequence body contains additional columns on top of what the sequence header defines.
                // We also need to identify the correct value type for the column. Since the column externalId and
                // value (type) are in different arrays, we must use an index based iteration.
                List<SequenceColumn> newColumns = new ArrayList<>();
                List<String> existingColumnExtIds = currentHeader.getColumnsList().stream()
                        .map(SequenceColumn::getExternalId)
                        .collect(Collectors.toList());
                for (int i = 0; i < entry.getValue().getColumnsCount(); i++) {
                    SequenceColumn column = entry.getValue().getColumns(i).toBuilder()
                            .putMetadata("source", "Java SDK upsert logic")
                            .build();

                    if (!existingColumnExtIds.contains(column.getExternalId())) {
                        // We have a new column. Identify the value type as well
                        Value columnValue = entry.getValue().getRows(0).getValues(i);
                        switch (columnValue.getKindCase()) {
                            case STRING_VALUE:
                                column = column.toBuilder().setValueType(SequenceColumn.ValueType.STRING).build();
                                break;
                            case NUMBER_VALUE:
                                column = column.toBuilder().setValueType(SequenceColumn.ValueType.DOUBLE).build();
                                break;
                            default:
                                throw new NoSuchElementException(String.format(loggingPrefix
                                + "Cannot identify a valid value type for the extra column extId %s for sequence %s. "
                                + "Inspecting column value gives KindCase: %.",
                                        column.getExternalId(),
                                        entry.getKey(),
                                        columnValue.getKindCase()));
                        }
                        newColumns.add(column);
                    }
                }
                if (newColumns.size() > 0) {
                    // We have new columns. Let's add them to the sequence header
                    currentHeader = currentHeader.toBuilder()
                            .addAllColumns(newColumns)
                            .build();
                    sequenceHeaderToUpdate.add(currentHeader);
                }
            }
            getClient().sequences().upsert(sequenceHeaderToUpdate);
            LOG.debug(loggingPrefix + "Updated the column schema for {} sequences", sequenceHeaderToUpdate.size());
        } else {
            LOG.warn(loggingPrefix + "Unable to identify missing column from CDF response: {}",
                    missingColumns);
        }
    }

    /**
     * Maps all items to their externalId (primary) or id (secondary). If the id function does not return any
     * identity, the item will be mapped to the empty string.
     *
     * Via the identity mapping, this function will also perform deduplication of the input items.
     *
     * @param items the sequence bodies to map to externalId / id.
     * @return the {@link Map} with all items mapped to externalId / id.
     */
    private Map<String, SequenceBody> mapToId(List<SequenceBody> items) {
        Map<String, SequenceBody> resultMap = new HashMap<>((int) (items.size() * 1.35));
        for (SequenceBody item : items) {
            if (item.hasExternalId()) {
                resultMap.put(item.getExternalId(), item);
            } else if (item.hasId()) {
                resultMap.put(String.valueOf(item.getId()), item);
            } else {
                resultMap.put("", item);
            }
        }
        return resultMap;
    }

    /**
     * Returns the number of columns for a sequence row.
     * @param sequenceRow
     * @return
     */
    private int getColumnsCount(SequenceRow sequenceRow) {
        return sequenceRow.getValuesCount();
    }

    /**
     * Returns the total count of characters in the (String) columns of a sequence body (collection of rows)
     * @param sequenceBody
     * @return
     */
    private int getCharacterCount(SequenceBody sequenceBody) {
        int characterCount = 0;
        for (SequenceRow sequenceRow : sequenceBody.getRowsList()) {
            characterCount += getCharacterCount(sequenceRow);
        }
        return characterCount;
    }

    /**
     * Returns the total count of characters in the (string) columns of a sequence row.
     * @param sequenceRow
     * @return
     */
    private int getCharacterCount(SequenceRow sequenceRow) {
        int characterCount = 0;
        for (Value value : sequenceRow.getValuesList()) {
            if (value.getKindCase() == Value.KindCase.STRING_VALUE) {
                characterCount += value.getStringValue().length();
            }
        }
        return characterCount;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private SequenceBody parseSequenceBody(String json) {
        try {
            return SequenceParser.parseSequenceBody(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of a sequence. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getSequenceId(SequenceBody item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract SequenceRows build();
    }
}
