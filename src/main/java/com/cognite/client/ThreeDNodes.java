package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDNodeParser;
import com.cognite.client.util.Partition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite 3D nodes api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDNode}.
 */
@AutoValue
public abstract class ThreeDNodes extends ApiBase {

    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructs a new {@link ThreeDNodes} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D models available outputs api object.
     */
    public static ThreeDNodes of(CogniteClient client) {
        return ThreeDNodes.builder()
                .setClient(client)
                .build();
    }

    private static ThreeDNodes.Builder builder() {
        return new AutoValue_ThreeDNodes.Builder();
    }

    /**
     * Returns all {@link ThreeDNode} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #list(Long,Long,Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId) throws Exception {
        return this.list(modelId, revisionId, Request.create());
    }

    /**
     * Returns all {@link ThreeDNode} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D nodes are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * PS: Full example in file <a href="https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/threeD.md"><b>threeD.md</b></a>
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Request requestParameters = Request.create()
     *           .withFilterParameter("properties", createFilterPropertiesWithCategories());
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId,requestParameters)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #list(Long,Long,Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param requestParameters the filters to use for retrieving the 3D nodes.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return this.list(modelId, revisionId, requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link ThreeDNode} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * PS: Full example in file <a href="https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/threeD.md"><b>threeD.md</b></a>
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Request requestParameters = Request.create()
     *           .withFilterParameter("properties", createFilterPropertiesWithCategories());
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId,requestParameters,
     *                              "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param requestParameters the filters to use for retrieving the 3D nodes..
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Request requestParameters, String... partitions) throws Exception {
        Request request = requestParameters
                .withRootParameter("modelId", modelId)
                .withRootParameter("revisionId", revisionId);
        return AdapterIterator.of(listJson(ResourceType.THREED_NODE, request, partitions), this::parseThreeDNodes);
    }

    /**
     * Returns all ancestor {@link ThreeDNode} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Long nodeId = 1L;
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId,nodeId)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #list(Long,Long,Long,Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param nodeId The id of ThreeDNode object
     * @return
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Long nodeId) throws Exception {
        return this.list(modelId, revisionId, nodeId, Request.create());
    }

    /**
     * Returns all ancestor {@link ThreeDNode} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D nodes are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Long nodeId = 1L;
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId,nodeId, Request.create().withRootParameter("limit", 300))
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #list(Long,Long,Long,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param nodeId The id of ThreeDNode object
     * @param requestParameters the filters to use for retrieving the 3D nodes.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Long nodeId, Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return this.list(modelId, revisionId, nodeId, requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all ancestor {@link ThreeDNode} objects that matches the filters set in the {@link Request} for the
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
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Long nodeId = 1L;
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .list(modelId,revisionId,nodeId, Request.create().withRootParameter("limit", 300),
     *                                                      "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodes">API Reference - List 3D nodes</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId The id of ThreeDModels object
     * @param revisionId The id of ThreeDModelRevision object
     * @param nodeId The id of ThreeDNode object
     * @param requestParameters the filters to use for retrieving the 3D nodes..
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Long nodeId, Request requestParameters, String... partitions) throws Exception {
        Request request = requestParameters
                .withRootParameter("modelId", modelId)
                .withRootParameter("revisionId", revisionId)
                .withRootParameter("nodeId", nodeId);
        return AdapterIterator.of(listJson(ResourceType.THREED_ANCESTOR_NODE, request, partitions), this::parseThreeDNodes);
    }

    /**
     * Retrieves 3D Nodes by ids.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long modelId = 1L;
     *      Long revisionId = 1L;
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<ThreeDNode> retrievedNodes =
     *                                      client
     *                                      .threeD()
     *                                      .models()
     *                                      .revisions()
     *                                      .nodes()
     *                                      .retrieve(modelId,revisionId,items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/get3DNodesById">API Reference - Get 3D nodes by ID</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId ID of ThreeDModel object
     * @param revisionId ID of ThreeDModelRevision object
     * @param items The item(s) {@code id} to retrieve
     * @return LIst of retrieved 3D Nodes.
     * @throws Exception
     */
    public List<ThreeDNode> retrieve(Long modelId, Long revisionId, List<Item> items) throws Exception {

        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDNodesById(modelId, revisionId);

        List<List<Item>> itemBatches = Partition.ofSize(items, 1000);
        Request requestTemplate = addAuthInfo(Request.create());

        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        for (List<Item> batch : itemBatches) {
            // build initial request object
            Request request = requestTemplate
                    .withItems(toRequestItems(batch));

            futureList.add(tdReader.getItemsAsync(request));
        }

        // Wait for all requests futures to complete
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the response items
        List<String> responseItems = new ArrayList<>(items.size());
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : futureList) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Retrieve 3d nodes by id failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItems.addAll(responseItemsFuture.join().getResultsItems());
        }

        return responseItems.stream()
                .map(this::parseThreeDNodes)
                .collect(Collectors.toList());
    }

    /**
     * Returns all {@link ThreeDNode} objects that matches the filters set in the {@link Request}.
     *
     * Endpoint is nodes/list
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D nodes are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .filter(modelId,revisionId)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/filter3DNodes">API Reference - Filter 3D nodes</a>
     *
     * @see #filter(Long,Long,Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId ID of ThreeDModel object
     * @param revisionId ID of ThreeDModelRevision object
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> filter(Long modelId, Long revisionId) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return this.filter(modelId, revisionId, Request.create(), partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link ThreeDNode} objects that matches the filters set in the {@link Request}.
     *
     * Endpoint is nodes/list
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D nodes are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * PS: Full example in file <a href="https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/threeD.md"><b>threeD.md</b></a>
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Request requestParameters = Request.create()
     *                             .withFilterParameter("properties", createFilterPropertiesWithCategories());
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .filter(modelId,revisionId,requestParameters)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/filter3DNodes">API Reference - Filter 3D nodes</a>
     *
     * @see #filter(Long,Long,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId ID of ThreeDModel object
     * @param revisionId ID of ThreeDModelRevision object
     * @param requestParameters the filters to use for retrieving the 3D nodes.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> filter(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return this.filter(modelId, revisionId, requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link ThreeDNode} objects that matches the filters set in the {@link Request}.
     *
     * Endpoint is nodes/list
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The 3D nodes are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * PS: Full example in file <a href="https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/threeD.md"><b>threeD.md</b></a>
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = 1L;
     *     Long revisionId = 1L;
     *     Request requestParameters = Request.create()
     *                             .withFilterParameter("properties", createFilterPropertiesWithCategories());
     *     List<ThreeDNode> listResults = new ArrayList<>();
     *     client.threeD()
     *             .models()
     *             .revisions()
     *             .nodes()
     *             .filter(modelId,revisionId,requestParameters,
     *                              "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Model-Revisions/operation/filter3DNodes">API Reference - Filter 3D nodes</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#nodes()
     *
     * @param modelId ID of ThreeDModel object
     * @param revisionId ID of ThreeDModelRevision object
     * @param requestParameters the filters to use for retrieving the 3D nodes.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> filter(Long modelId, Long revisionId, Request requestParameters, String... partitions) throws Exception {
        String loggingPrefix = "filter() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        Request requestParams = addAuthInfo(requestParameters);

        // Build the api iterators.
        List<Iterator<CompletableFuture<ResponseItems<String>>>> iterators = new ArrayList<>();
        if (partitions.length < 1) {
            // No partitions specified. Just pass on the request parameters without any modification
            LOG.debug("No partitions specified. Will use a single read iterator stream.");
            iterators.add(connector.filterThreeDNodes(modelId, revisionId, requestParams));
        } else {
            // We have some partitions. Add them to the request parameters and get the iterators
            LOG.debug(String.format("Identified %d partitions. Will use a separate iterator stream per partition.",
                    partitions.length));
            for (String partition : partitions) {
                Request requestWithParticion = requestParams.withRootParameter("partition", partition);
                iterators.add(connector.filterThreeDNodes(modelId, revisionId, requestWithParticion));
            }
        }

        return AdapterIterator.of(FanOutIterator.of(iterators), this::parseThreeDNodes);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDNode> parseThreeDNodesToList(String json) {
        try {
            return ThreeDNodeParser.parseThreeDNodesToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ThreeDNode parseThreeDNodes(String json) {
        try {
            return ThreeDNodeParser.parseThreeDNodes(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDNodes.Builder> {
        abstract ThreeDNodes build();
    }
}
