package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoValue
public abstract class ThreeDAssetMappingsFilterRequestProvider extends GenericRequestProvider {

    private ObjectMapper objectMapper = new ObjectMapper();

    public static ThreeDAssetMappingsFilterRequestProvider.Builder builder() {
        return new AutoValue_ThreeDAssetMappingsFilterRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDAssetMappingsFilterRequestProvider.Builder toBuilder();

    public ThreeDAssetMappingsFilterRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");

        if (parameters.getRequestParameters().containsKey("filter")) {
            Map<String, Object> filter =
                    (Map<String, Object>) parameters.getRequestParameters().get("filter");

            if (filter.containsKey("assetIds")) {
                Preconditions.checkArgument(filter.get("assetIds") instanceof List,
                        "Request parameters must include assetIds with a list long value");
            }
            if (filter.containsKey("nodeIds")) {
                Preconditions.checkArgument(filter.get("nodeIds") instanceof List,
                        "Request parameters must include nodeIds with a list long value");
            }
            if (filter.containsKey("treeIndexes")) {
                Preconditions.checkArgument(filter.get("treeIndexes") instanceof List,
                        "Request parameters must include treeIndexes with a list long value");
            }
        }
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        withRequest(requestParameters);
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            requestParameters = requestParameters.withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE);
        }

        if (cursor.isPresent()) {
            requestParameters = requestParameters.withRootParameter("cursor", cursor.get());
        }

        String outputJson = requestParameters.getRequestParametersAsJson();
        okhttp3.Request.Builder build = requestBuilder.url(urlBuilder.build());

        return build.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDAssetMappingsFilterRequestProvider.Builder> {
        public abstract ThreeDAssetMappingsFilterRequestProvider build();
    }

}
