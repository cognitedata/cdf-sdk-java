package com.cognite.client;

import com.cognite.client.dto.ThreeDOutput;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDOutputsParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite 3D models available outputs api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDOutput}.
 */
@AutoValue
public abstract class ThreeDOutputs extends ApiBase {

    /**
     * Constructs a new {@link ThreeDOutputs} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D models available outputs api object.
     */
    public static ThreeDOutputs of(CogniteClient client) {
        return ThreeDOutputs.builder()
                .setClient(client)
                .build();
    }

    private static ThreeDOutputs.Builder builder() {
        return new AutoValue_ThreeDOutputs.Builder();
    }

    /**
     * Retrieves 3D Available Outputs by modeId and revisionId
     *
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     */
    public List<ThreeDOutput> retrieve(Long modelId, Long revisionId) throws Exception {
        return retrieve(modelId, revisionId, Request.create());
    }

    public List<ThreeDOutput> retrieve(Long modelId, Long revisionId, Request requestParameters) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDOutputs();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        Request request = requestParameters
                .withRootParameter("modelId", modelId)
                .withRootParameter("revisionId", revisionId);
        resultFutures.add(tdReader.getItemsAsync(addAuthInfo(request)));
        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<ResponseItems<String>> itemsAsync = tdReader.getItemsAsync(addAuthInfo(request));
        itemsAsync.join();

        // Collect the response items
        if (!itemsAsync.join().isSuccessful()) {
            // something went wrong with the request
            String message = loggingPrefix + "Retrieve 3D Available Outputs failed: "
                    + itemsAsync.join().getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }

        return parseThreeDOutputs(itemsAsync.join().getResponseBodyAsString());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDOutput> parseThreeDOutputs(String json) {
        try {
            return ThreeDOutputsParser.parseThreeDOutputs(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDOutputs.Builder> {
        abstract ThreeDOutputs build();
    }
}
