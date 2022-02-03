package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoValue
public abstract class ThreeDAssetMappingsRequestProvider extends GenericRequestProvider {

    private ObjectMapper objectMapper = new ObjectMapper();

    public static ThreeDAssetMappingsRequestProvider.Builder builder() {
        return new AutoValue_ThreeDAssetMappingsRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDAssetMappingsRequestProvider.Builder toBuilder();

    public ThreeDAssetMappingsRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("modelId")
                        && parameters.getRequestParameters().get("modelId") instanceof Long,
                "Request parameters must include modelId with a Long value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("revisionId")
                        && parameters.getRequestParameters().get("revisionId") instanceof Long,
                "Request parameters must include revisionId with a Long value");

        if (parameters.getRequestParameters().containsKey("intersectsBoundingBox")) {
            ThreeDNode.BoundingBox paramBoundingBox = (ThreeDNode.BoundingBox) parameters.getRequestParameters().get("intersectsBoundingBox");
            Preconditions.checkArgument(!paramBoundingBox.getMinList().isEmpty(),
                    "Request parameters must include intersectsBoundingBox.min with a list double value");
            Preconditions.checkArgument(!paramBoundingBox.getMaxList().isEmpty(),
                    "Request parameters must include intersectsBoundingBox.max with a list double value");
        }


        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        withRequest(requestParameters);
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder.addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("modelId")));
        urlBuilder.addPathSegment("revisions");
        urlBuilder.addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("revisionId")));
        urlBuilder.addPathSegment("mappings");

        String limit = "";
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            limit = String.valueOf(ConnectorConstants.DEFAULT_MAX_BATCH_SIZE);
        } else {
            limit = requestParameters.getRequestParameters().get("limit").toString();
        }
        urlBuilder.addQueryParameter("limit", limit);

        if (cursor.isPresent()) {
            urlBuilder.addQueryParameter("cursor", cursor.get());
        }

        if (requestParameters.getRequestParameters().containsKey("nodeId")) {
            urlBuilder.addQueryParameter("nodeId", String.valueOf(requestParameters.getRequestParameters().get("nodeId")));
        } else if (requestParameters.getRequestParameters().containsKey("assetId")) {
            urlBuilder.addQueryParameter("assetId", String.valueOf(requestParameters.getRequestParameters().get("assetId")));
        } else if (requestParameters.getRequestParameters().containsKey("intersectsBoundingBox")) {
            ThreeDNode.BoundingBox paramBoundingBox = (ThreeDNode.BoundingBox) requestParameters.getRequestParameters().get("intersectsBoundingBox");
            Map<String, List<Double>> values = new HashMap<>();
            values.put("min", paramBoundingBox.getMinList());
            values.put("max", paramBoundingBox.getMaxList());
            String boundingBox = objectMapper.writeValueAsString(values);
            urlBuilder.addQueryParameter("intersectsBoundingBox", boundingBox);
        }

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDAssetMappingsRequestProvider.Builder> {
        public abstract ThreeDAssetMappingsRequestProvider build();
    }

}
