package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDNodeParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite 3D nodes api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDNode}.
 */
@AutoValue
public abstract class ThreeDNodes extends ApiBase {

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
     * @see #list(Long, Long, Request)
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
     * @param requestParameters the filters to use for retrieving the 3D nodes..
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ThreeDNode>> list(Long modelId, Long revisionId, Request requestParameters, String... partitions) throws Exception {
        Request request = requestParameters
                .withRootParameter("modelId", String.valueOf(modelId))
                .withRootParameter("revisionId", String.valueOf(revisionId));
        return AdapterIterator.of(listJson(ResourceType.THREED_NODE, request, partitions), this::parseThreeDNodes);
    }

    /**
     * Retrieves 3D nodes by modeId and revisionId
     *
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     */
    public List<ThreeDNode> retrieve(Long modelId, Long revisionId) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDNodes();

        Request request = Request.create()
                .withRootParameter("modelId", String.valueOf(modelId))
                .withRootParameter("revisionId", String.valueOf(revisionId));

        CompletableFuture<ResponseItems<String>> itemsAsync = tdReader.getItemsAsync(addAuthInfo(request));
        ResponseItems<String> responseItems = itemsAsync.join();

        List<ThreeDNode> listResponse = new ArrayList<>();
        if (!responseItems.isSuccessful()) {
            // something went wrong with the request
            String message = loggingPrefix + "Retrieve 3d model failed: "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        } else {
            listResponse.addAll(parseThreeDNodesToList(responseItems.getResponseBodyAsString()));
        }

        return listResponse;
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
