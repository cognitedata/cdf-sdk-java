package com.cognite.client;

import com.cognite.client.dto.ThreeDAvailableOutput;
import com.cognite.client.dto.ThreeDRevisionLog;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDAvailableOutputsParser;
import com.cognite.client.servicesV1.parser.ThreeDRevisionLogsParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
     * @param modelId The id of ThreeDModel object
     * @param revisionId The id of ThreeDModelRevision object
     */
    public List<ThreeDRevisionLog> retrieve(Long modelId, Long revisionId) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> tdReader = connector.readThreeDRevisionLogs();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        Request request = Request.create()
                .withRootParameter("modelId", String.valueOf(modelId))
                .withRootParameter("revisionId", String.valueOf(revisionId));
        resultFutures.add(tdReader.getItemsAsync(addAuthInfo(request)));
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
                String message = loggingPrefix + "Retrieve 3d revision logs failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItems.add(responseItemsFuture.join().getResponseBodyAsString());
        }

        return responseItems.stream()
                .map(this::parseThreeDRevisionLog)
                .collect(Collectors.toList());
    }

    /*
   Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
   deal very well with exceptions.
    */
    private ThreeDRevisionLog parseThreeDRevisionLog(String json) {
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
