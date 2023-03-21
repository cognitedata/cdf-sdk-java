package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDModelParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite 3D models api endpoint.
 *
 * It provides methods for reading and writing {@link com.cognite.client.dto.ThreeDModel}.
 */
@AutoValue
public abstract class ThreeDModels extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(ThreeDModels.class);

    private final static int MAX_UPSERT_BATCH_SIZE = 200;

    /**
     * Constructs a new {@link ThreeDModels} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D models api object.
     */
    public static ThreeDModels of(CogniteClient client) {
        return ThreeDModels.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link ThreeDModelsRevisions} representing 3D Models Revisions api endpoints.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.threeD().models().revisions();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     *
     * @return The ThreeDModelsRevisions api endpoints.
     */
    public ThreeDModelsRevisions revisions() {
        return ThreeDModelsRevisions.of(getClient());
    }

    private static ThreeDModels.Builder builder() {
        return new AutoValue_ThreeDModels.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract ThreeDModels build();
    }

    /**
     * Returns all {@link ThreeDModel} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<ThreeDModel> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .list()
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/get3DModels">API Reference - List 3D models</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     */
    public Iterator<List<ThreeDModel>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link ThreeDModel} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D models are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<ThreeDModel> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .list(Request.create()
     *                          .withRootParameter("published", true))
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/get3DModels">API Reference - List 3D models</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     *
     * @param requestParameters the filters to use for retrieving the 3D models.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDModel>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[0]));
    }

    /**
     * Returns all {@link ThreeDModel} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ThreeDModel> listResults = new ArrayList<>();
     *      client.threeD()
     *              .models()
     *              .list(Request.create()
     *                             .withFilterParameter("published", true),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/get3DModels">API Reference - List 3D models</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     *
     * @param requestParameters the filters to use for retrieving the 3d models.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDModel>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.THREED_MODEL, requestParameters, partitions), this::parseThreeDModel);
    }

    /**
     * Retrieves 3D Models by id.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<ThreeDModel> retrievedThreeDModel = client.threeD().models().retrieve(items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/get3DModel">API Reference - Retrieve a 3D model</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     *
     * @param items The item(s) {@code id} to retrieve.
     * @return The retrieved 3D Models.
     * @throws Exception
     */
    public List<ThreeDModel> retrieve(List<Item> items) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDModelsById();

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
                .map(this::parseThreeDModel)
                .collect(Collectors.toList());
    }

    /**
     * Creates or update a set of {@link ThreeDModel} objects.
     *
     * If it is a new {@link ThreeDModel} object (based on the {@code id}, then it will be created.
     *
     * If an {@link ThreeDModel} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ThreeDModel> threeDModels = // List of ThreeDModels;
     *      client.threeD().models().upsert(threeDModels);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/create3DModels">API Reference - Create 3D models</a><br/>
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/update3DModels">API Reference - Update 3D models</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     *
     * @param threeDModels The 3D Models to upsert
     * @return The upserted 3D Models
     * @throws Exception
     */
    public List<ThreeDModel> upsert(List<ThreeDModel> threeDModels) throws Exception {
        String loggingPrefix = "upsert() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeThreeDModels();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateThreeDModels();

        List<Map<String, Object>> insertItems = new ArrayList<>();
        List<Map<String, Object>> updateItems = new ArrayList<>();

        threeDModels.forEach(td-> {
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
        List<ThreeDModel> listResponse = new ArrayList<>();
        String exceptionMessage = "";
        for (CompletableFuture<ResponseItems<String>> future : futureList) {
            ResponseItems<String> response = future.get();
            if (response.isSuccessful()) {
                listResponse.addAll(parseThreeDModelToList(response.getResponseBodyAsString()));
                LOG.debug(loggingPrefix + "Upsert items request success. Adding batch to result collection.");
            } else {
                exceptionMessage = response.getResponseBodyAsString();
                LOG.debug(loggingPrefix + "Upsert items request failed: {}", response.getResponseBodyAsString());
            }
        }
        return listResponse;
    }

    /**
     *
     * Deletes a set of ThreeDModel.
     * <p>
     * The threeDModels to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> threeDModels = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deletedItemsResults = client.threeD().models().delete(threeDModels);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Models/operation/delete3DModels">API Reference - Delete 3D models</a>
     *
     * @see DeleteItems#deleteItems(List)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     *
     * @param threeDModels
     * @return
     * @throws Exception
     */
    public List<Item> delete(List<Item> threeDModels) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteThreeDModels();

        DeleteItems deleteItems = DeleteItems.ofItem(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(threeDModels);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ThreeDModel parseThreeDModel(String json) {
        try {
            return ThreeDModelParser.parseThreeDModel(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }


    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDModel> parseThreeDModelToList(String json) {
        try {
            return ThreeDModelParser.parseThreeDModelToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

//    /*
//    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
//    deal very well with exceptions.
//     */
//    private List<ThreeDModel> parseThreeDModelToList(String json) {
//        try {
//            return ThreeDModelParser.parseThreeDModelToList(json);
//        } catch (Exception e)  {
//            throw new RuntimeException(e);
//        }
//    }

    /*
   Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
   deal very well with exceptions.
    */
    private Map<String, Object> toRequestInsertItem(ThreeDModel element) {
        try {
            return ThreeDModelParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(ThreeDModel item) {
        try {
            return ThreeDModelParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(ThreeDModel item) {
        try {
            return ThreeDModelParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

}
