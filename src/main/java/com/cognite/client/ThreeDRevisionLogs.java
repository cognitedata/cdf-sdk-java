package com.cognite.client;

import com.cognite.client.dto.ThreeDRevisionLog;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDRevisionLogsParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite 3D revision logs api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDRevisionLog}.
 */
@AutoValue
public abstract class ThreeDRevisionLogs extends ApiBase {

    /**
     * Constructs a new {@link ThreeDRevisionLogs} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D revision logs api object.
     */
    public static ThreeDRevisionLogs of(CogniteClient client) {
        return ThreeDRevisionLogs.builder()
                .setClient(client)
                .build();
    }

    private static ThreeDRevisionLogs.Builder builder() {
        return new AutoValue_ThreeDRevisionLogs.Builder();
    }

    /**
     * Retrieves 3D Revision Logs by modeId and revisionId
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long modelId = 1L;
     *      Long revisionId = 1L;
     *      List<ThreeDOutput> retrievedThreeDOutputs =
     *                                      client
     *                                      .threeD()
     *                                      .models()
     *                                      .revisions()
     *                                      .revisionLogs()
     *                                      .retrieve(modelId,revisionId);
     * }
     * </pre>
     *
     * @see #retrieve(Long,Long,Request)
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#revisionLogs()
     *
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     */
    public List<ThreeDRevisionLog> retrieve(Long modelId, Long revisionId) throws Exception {
       return retrieve(modelId, revisionId, Request.create());
    }

    /**
     *
     * Retrieves 3D Revision Logs by modeId and revisionId
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long modelId = 1L;
     *      Long revisionId = 1L;
     *      Request requestParameters = Request.create().withRootParameter("severity", Long.valueOf(3));
     *      List<ThreeDOutput> retrievedThreeDOutputs =
     *                                      client
     *                                      .threeD()
     *                                      .models()
     *                                      .revisions()
     *                                      .revisionLogs()
     *                                      .retrieve(modelId,revisionId,requestParameters);
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#models()
     * @see ThreeDModels#revisions()
     * @see ThreeDModelsRevisions#revisionLogs()
     *
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     * @param requestParameters The filters to use for retrieving the 3D Revision Logs.
     * @return
     * @throws Exception
     */
    public List<ThreeDRevisionLog> retrieve(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDRevisionLogs();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        Request request = requestParameters
                .withRootParameter("modelId", modelId)
                .withRootParameter("revisionId", revisionId);
        resultFutures.add(tdReader.getItemsAsync(addAuthInfo(request)));
        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<ResponseItems<String>> itemsAsync = tdReader.getItemsAsync(addAuthInfo(request));
        ResponseItems<String> responseItems = itemsAsync.join();

        // Collect the response items
        if (!responseItems.isSuccessful()) {
            // something went wrong with the request
            String message = loggingPrefix + "Retrieve 3d revision logs failed: "
                    + itemsAsync.join().getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }

        return parseThreeDRevisionLog(responseItems.getResponseBodyAsString());
    }

    /*
   Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
   deal very well with exceptions.
    */
    private List<ThreeDRevisionLog> parseThreeDRevisionLog(String json) {
        try {
            return ThreeDRevisionLogsParser.parseThreeDRevisionLogs(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDRevisionLogs.Builder> {
        abstract ThreeDRevisionLogs build();
    }
}
