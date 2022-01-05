package com.cognite.client;

import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDNodeParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite 3D nodes api endpoint.
 *
 * It provides methods for reading {@link com.cognite.client.dto.ThreeDNode}.
 */
@AutoValue
public abstract class ThreeDNodes extends ApiBase{

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
            listResponse.addAll(parseThreeDNodes(responseItems.getResponseBodyAsString()));
        }

        return listResponse;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<ThreeDNode> parseThreeDNodes(String json) {
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
