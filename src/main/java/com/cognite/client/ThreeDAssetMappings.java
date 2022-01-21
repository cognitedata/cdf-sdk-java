package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDAssetMapping;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDAssetMappingsParser;
import com.cognite.client.servicesV1.parser.ThreeDModelRevisionParser;
import com.cognite.client.servicesV1.parser.ThreeDNodeParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite 3D asset mappings api endpoint.
 *
 * It provides methods for reading and writing {@link com.cognite.client.dto.ThreeDModelRevision}.
 */
@AutoValue
public abstract class ThreeDAssetMappings extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(ThreeDAssetMappings.class);

    /**
     * Constructs a new {@link ThreeDAssetMappings} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D asset mappings api object.
     */
    public static ThreeDAssetMappings of(CogniteClient client) {
        return ThreeDAssetMappings.builder()
                .setClient(client)
                .build();
    }

    private static ThreeDAssetMappings.Builder builder() {
        return new AutoValue_ThreeDAssetMappings.Builder();
    }

    /**
     * Returns all {@link ThreeDAssetMapping} objects.
     *
     * @see #list(Long, Long, Request)
     */
    public Iterator<List<ThreeDAssetMapping>> list(Long modelId, Long revisionId) throws Exception {
        return this.list(modelId, revisionId, Request.create());
    }

    /**
     * Returns all {@link ThreeDAssetMapping} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D asset mappings are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the 3D asset mappings.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDAssetMapping>> list(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return this.list(modelId, revisionId, requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link ThreeDAssetMapping} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the 3D asset mappings
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDAssetMapping>> list(Long modelId, Long revisionId, Request requestParameters, String... partitions) throws Exception {
        Request request = requestParameters
                .withRootParameter("modelId", String.valueOf(modelId))
                .withRootParameter("revisionId", String.valueOf(revisionId));
        return AdapterIterator.of(listJson(ResourceType.THREED_ASSET_MAPPINGS, request, partitions), this::parseThreeDAssetMapping);
    }

    /**
     * Returns all {@link ThreeDAssetMapping} objects.
     *
     * @see #filter(Long, Long, Request)
     */
    public Iterator<List<ThreeDAssetMapping>> filter(Long modelId, Long revisionId) throws Exception {
        return this.filter(modelId, revisionId, Request.create());
    }

    /**
     * Returns all {@link ThreeDAssetMapping} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D asset mappings are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the 3D asset mappings.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDAssetMapping>> filter(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        String loggingPrefix = "filter() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        Request requestParams = addAuthInfo(requestParameters);

        // Build the api iterators.
        List<Iterator<CompletableFuture<ResponseItems<String>>>> iterators = new ArrayList<>();
        iterators.add(connector.filterThreeDAssetMappings(modelId, revisionId, requestParams));
        return AdapterIterator.of(FanOutIterator.of(iterators), this::parseThreeDAssetMapping);
    }

    /**
     * Creates a set of {@link ThreeDAssetMapping} objects.
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param items The 3D asset mappings to upsert
     * @return The upserted 3D asset mappings
     * @throws Exception
     */
    public List<ThreeDAssetMapping> create(Long modelId, Long revisionId, List<ThreeDAssetMapping> items) throws Exception {
        String loggingPrefix = "create() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeThreeDAssetMappings(modelId, revisionId);

        List<Map<String, Object>> insertItems = new ArrayList<>();
        items.forEach(item -> insertItems.add(toRequestInsertItem(item)));

        Request requestInsert = Request.create().withItems(insertItems);

        CompletableFuture<ResponseItems<String>> responseInsert =
                createItemWriter.writeItemsAsync(addAuthInfo(requestInsert));

        List<ThreeDAssetMapping> listResponse = new ArrayList<>();
        ResponseItems<String> response = responseInsert.join();
        if (response.isSuccessful()) {
            listResponse.addAll(parseThreeDAssetMappingToList(response.getResponseBodyAsString()));
        } else {
            String exceptionMessage = response.getResponseBodyAsString();
            LOG.debug(loggingPrefix + "Create items request failed: {}", exceptionMessage);
        }
        return listResponse;
    }

    /**
     *  Deletes 3D asset mappings.
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param deleteItemsInput List of {@link Item} containing the ids of 3D asset mappings to delete
     * @return
     * @throws Exception
     */
    public Boolean delete(Long modelId, Long revisionId, List<ThreeDAssetMapping> deleteItemsInput) throws Exception {
        String loggingPrefix = "delete() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteThreeDAssetMappings(modelId,revisionId);

        List<Map<String, Object>> deleteItems = new ArrayList<>();
        deleteItemsInput.forEach(item -> deleteItems.add(toRequestDeleteItem(item)));

        Request requestDelete = Request.create().withItems(deleteItems);

        CompletableFuture<ResponseItems<String>> responseDelete =
                deleteItemWriter.writeItemsAsync(addAuthInfo(requestDelete));

        ResponseItems<String> response = responseDelete.join();
        if (response.isSuccessful()) {
           return true;
        } else {
            String exceptionMessage = response.getResponseBodyAsString();
            LOG.debug(loggingPrefix + "Deleted items request failed: {}", exceptionMessage);
            return false;
        }
    }

    private Map<String, Object> toRequestDeleteItem(ThreeDAssetMapping element) {
        try {
            return ThreeDAssetMappingsParser.toRequestDeleteItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestInsertItem(ThreeDAssetMapping element) {
        try {
            return ThreeDAssetMappingsParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ThreeDAssetMapping parseThreeDAssetMapping(String json) {
        try {
            return ThreeDAssetMappingsParser.parseThreeDAssetMapping(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDAssetMapping> parseThreeDAssetMappingToList(String json) {
        try {
            return ThreeDAssetMappingsParser.parseThreeDAssetMappingToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDAssetMappings.Builder> {
        abstract ThreeDAssetMappings build();
    }
}
