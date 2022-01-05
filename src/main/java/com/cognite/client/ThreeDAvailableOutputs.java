package com.cognite.client;

import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDAvailableOutput;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDAvailableOutputsParser;
import com.cognite.client.servicesV1.parser.ThreeDModelRevisionParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite 3D models available outputs api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDAvailableOutput}.
 */
@AutoValue
public abstract class ThreeDAvailableOutputs extends ApiBase {

    /**
     * Constructs a new {@link ThreeDAvailableOutputs} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the 3D models available outputs api object.
     */
    public static ThreeDAvailableOutputs of(CogniteClient client) {
        return ThreeDAvailableOutputs.builder()
                .setClient(client)
                .build();
    }

    private static ThreeDAvailableOutputs.Builder builder() {
        return new AutoValue_ThreeDAvailableOutputs.Builder();
    }

    /**
     * Retrieves 3D Available Outputs by modeId and revisionId
     *
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     */
    public List<ThreeDAvailableOutput> retrieve(Long modelId, Long revisionId) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDAvailableOutputs();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        Request request = Request.create()
                .withRootParameter("modelId", String.valueOf(modelId))
                .withRootParameter("revisionId", String.valueOf(revisionId));
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

        return parseThreeDAvailableOutputs(itemsAsync.join().getResponseBodyAsString());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDAvailableOutput> parseThreeDAvailableOutputs(String json) {
        try {
            return ThreeDAvailableOutputsParser.parseThreeDAvailableOutputs(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDAvailableOutputs.Builder> {
        abstract ThreeDAvailableOutputs build();
    }
}
