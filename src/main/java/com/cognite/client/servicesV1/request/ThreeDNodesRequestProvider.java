package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class ThreeDNodesRequestProvider extends GenericRequestProvider {

    public static ThreeDNodesRequestProvider.Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_ThreeDNodesRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDNodesRequestProvider.Builder toBuilder();

    public ThreeDNodesRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("modelId")
                && parameters.getRequestParameters().get("modelId") instanceof Long,
                "Request parameters must include modelId with a Long value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("revisionId")
                && parameters.getRequestParameters().get("revisionId") instanceof Long,
                "Request parameters must include revisionId with a Long value");

        if (parameters.getRequestParameters().containsKey("depth")) {
            Preconditions.checkArgument(parameters.getRequestParameters().get("depth") instanceof Integer,
                    "Request parameters must include depth with a Integer value");
        }

        if (parameters.getRequestParameters().containsKey("nodeId")) {
            Preconditions.checkArgument(parameters.getRequestParameters().get("nodeId") instanceof Long,
                    "Request parameters must include nodeId with a Long value");
        }

        if (parameters.getRequestParameters().containsKey("sortByNodeId")) {
            Preconditions.checkArgument(parameters.getRequestParameters().get("sortByNodeId") instanceof Boolean,
                    "Request parameters must include sortByNodeId with a Boolean value");
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
        urlBuilder.addPathSegment("nodes");

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

        if (requestParameters.getRequestParameters().containsKey("depth")) {
            urlBuilder.addQueryParameter("depth", String.valueOf(requestParameters.getRequestParameters().get("depth")));
        }

        if (requestParameters.getRequestParameters().containsKey("nodeId")) {
            urlBuilder.addQueryParameter("nodeId", String.valueOf(requestParameters.getRequestParameters().get("nodeId")));
        }

        if (requestParameters.getRequestParameters().containsKey("sortByNodeId")) {
            urlBuilder.addQueryParameter("sortByNodeId", String.valueOf(requestParameters.getRequestParameters().get("sortByNodeId")));
        }

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDNodesRequestProvider.Builder> {
        public abstract ThreeDNodesRequestProvider build();
    }

}
