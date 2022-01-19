package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDModelRevisionParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite 3D models revisions api endpoint.
 *
 * It provides methods for reading and writing {@link com.cognite.client.dto.ThreeDModelRevision}.
 */
@AutoValue
public abstract class ThreeDModelsRevisions extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(ThreeDModelsRevisions.class);

    /**
     * Constructs a new {@link ThreeDModelsRevisions} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D models api object.
     */
    public static ThreeDModelsRevisions of(CogniteClient client) {
        return ThreeDModelsRevisions.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link ThreeDOutputs} representing 3D Models available outputs api endpoints.
     *
     * @return The ThreeDOutputs api endpoints.
     */
    public ThreeDOutputs outputs() {
        return ThreeDOutputs.of(getClient());
    }

    /**
     * Returns {@link ThreeDRevisionLogs} representing 3D Revision Logs api endpoints.
     *
     * @return The ThreeDRevisionLogs api endpoints.
     */
    public ThreeDRevisionLogs revisionLogs() {
        return ThreeDRevisionLogs.of(getClient());
    }

    /**
     * Returns {@link ThreeDNodes} representing 3D nodes api endpoints.
     *
     * @return The ThreeDAvailableOutputs api endpoints.
     */
    public ThreeDNodes nodes() {
        return ThreeDNodes.of(getClient());
    }

    private static ThreeDModelsRevisions.Builder builder() {
        return new AutoValue_ThreeDModelsRevisions.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDModelsRevisions.Builder> {
        abstract ThreeDModelsRevisions build();
    }

    /**
     * Returns all {@link ThreeDModelRevision} objects.
     *
     * @param modelId the id of 3d models
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDModelRevision>> list(Long modelId) throws Exception {
        return this.list(modelId, Request.create());
    }

    /**
     * Returns all {@link ThreeDModelRevision} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D models are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the 3D models revisions.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDModelRevision>> list(Long modelId, Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(modelId, requestParameters, partitions.toArray(new String[0]));
    }

    /**
     * Returns all {@link ThreeDModelRevision} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param modelId The id of ThreeDModels object
     * @param requestParameters the filters to use for retrieving the 3d models revisions.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDModelRevision>> list(Long modelId, Request requestParameters, String... partitions) throws Exception {
        Request request = requestParameters.withRootParameter("modelId", modelId);
        return AdapterIterator.of(listJson(ResourceType.THREED_MODEL_REVISION, request, partitions), this::parseThreeDModelRevision);
    }

    /**
     * Retrieves 3D Models by id.
     *
     * @param modelId The id of ThreeDModels object
     * @param items The item(s) {@code id} to retrieve.
     * @return The retrieved 3D Models.
     * @throws Exception
     */
    public List<ThreeDModelRevision> retrieve(Long modelId, List<Item> items) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDModelsRevisionsById(modelId);

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        for (Item item : items) {
            Request request = Request.create().withRootParameter("id", item.getId());
            resultFutures.add(tdReader.getItemsAsync(addAuthInfo(request)));
        }
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
                String message = loggingPrefix + "Retrieve 3d model failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItems.add(responseItemsFuture.join().getResponseBodyAsString());
        }

        return responseItems.stream()
                .map(this::parseThreeDModelRevision)
                .collect(Collectors.toList());
    }

    /**
     * Creates or update a set of {@link ThreeDModelRevision} objects.
     *
     * If it is a new {@link ThreeDModelRevision} object (based on the {@code id}, then it will be created.
     *
     * If an {@link ThreeDModelRevision} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param modelId The id of ThreeDModels object
     * @param threeDModelRevisions The 3D Model Revisions to upsert
     * @return The upserted 3D Models Revisions
     * @throws Exception
     */
    public List<ThreeDModelRevision> upsert(Long modelId, List<ThreeDModelRevision> threeDModelRevisions) throws Exception {
        String loggingPrefix = "upsert() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeThreeDModelsRevisions(modelId);
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateThreeDModelsRevisions(modelId);

        List<Map<String, Object>> insertItems = new ArrayList<>();
        List<Map<String, Object>> updateItems = new ArrayList<>();

        threeDModelRevisions.forEach(td-> {
            if (td.getId() == 0) {
                insertItems.add(toRequestInsertItem(td));
            } else {
                if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
                    updateItems.add(toRequestReplaceItem(td));
                } else {
                    updateItems.add(toRequestUpdateItem(td));
                }
            }
        });

        Request requestInsert = Request.create().withItems(insertItems);
        Request requestUpdate = Request.create().withItems(updateItems);

        CompletableFuture<ResponseItems<String>> responseInsert =
                createItemWriter.writeItemsAsync(addAuthInfo(requestInsert));
        CompletableFuture<ResponseItems<String>> responseUpdate =
                updateItemWriter.writeItemsAsync(addAuthInfo(requestUpdate));

        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        if (insertItems.size() > 0) {
            futureList.add(responseInsert);
        }
        if (updateItems.size() > 0) {
            futureList.add(responseUpdate);
        }

        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        //Collect the responses from the futures
        List<ThreeDModelRevision> listResponse = new ArrayList<>();
        String exceptionMessage = "";
        for (CompletableFuture<ResponseItems<String>> future : futureList) {
            ResponseItems<String> response = future.get();
            if (response.isSuccessful()) {
                listResponse.addAll(parseThreeDModelToList(response.getResponseBodyAsString()));
                LOG.debug(loggingPrefix + "Upsert items request success. Adding batch to result collection.");
            } else {
                exceptionMessage = response.getResponseBodyAsString();
                LOG.debug(loggingPrefix + "Upsert items request failed: {}", exceptionMessage);
            }
        }
        return listResponse;
    }


    /**
     *  Deletes 3D Revisions.
     *
     * @param modelId The id of ThreeDModels object
     * @param deleteItemsInput List of {@link Item} containing the ids of 3D Model Revisions to delete
     * @return
     * @throws Exception
     */
    public List<Item> delete(Long modelId, List<Item> deleteItemsInput) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteThreeDModelsRevisions(modelId);

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(deleteItemsInput);
    }

    /**
     * Update the Thumbnail of Revision
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param thumbnailId The id of Thumbnail file
     * @return true if is updated or false to not updated
     * @throws Exception
     */
    public Boolean updateThumbnail(Long modelId, Long revisionId, Long thumbnailId) throws Exception {
        String loggingPrefix = "updateThumbnail() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateThreeDTRevisionThumbnail(modelId, revisionId);

        Request request = Request.create().withRootParameter("fileId", thumbnailId);
        CompletableFuture<ResponseItems<String>> responseFuture =
                updateItemWriter.writeItemsAsync(addAuthInfo(request));
        responseFuture.join();

        ResponseItems<String> response = responseFuture.get();
        if (!response.isSuccessful()) {
            String exceptionMessage = response.getResponseBodyAsString();
            LOG.debug(loggingPrefix + "Update thumbnail request failed: {}", exceptionMessage);
            return false;
        }
        return true;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ThreeDModelRevision parseThreeDModelRevision(String json) {
        try {
            return ThreeDModelRevisionParser.parseThreeDModelRevision(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDModelRevision> parseThreeDModelToList(String json) {
        try {
            return ThreeDModelRevisionParser.parseThreeDModelToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestInsertItem(ThreeDModelRevision element) {
        try {
            return ThreeDModelRevisionParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestUpdateItem(ThreeDModelRevision element) {
        try {
            return ThreeDModelRevisionParser.toRequestUpdateItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestReplaceItem(ThreeDModelRevision element) {
        try {
            return ThreeDModelRevisionParser.toRequestReplaceItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }
}
