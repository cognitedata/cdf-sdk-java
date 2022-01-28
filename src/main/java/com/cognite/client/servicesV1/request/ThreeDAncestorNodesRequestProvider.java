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
public abstract class ThreeDAncestorNodesRequestProvider extends GenericRequestProvider {

    public static ThreeDAncestorNodesRequestProvider.Builder builder() {
        return new AutoValue_ThreeDAncestorNodesRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDAncestorNodesRequestProvider.Builder toBuilder();

    public ThreeDAncestorNodesRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("modelId")
                        && parameters.getRequestParameters().get("modelId") instanceof String,
                "Request parameters must include modelId with a string value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("revisionId")
                        && parameters.getRequestParameters().get("revisionId") instanceof String,
                "Request parameters must include revisionId with a string value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("nodeId")
                        && parameters.getRequestParameters().get("nodeId") instanceof String,
                "Request parameters must include nodeId with a string value");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        withRequest(requestParameters);
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("modelId"));
        urlBuilder.addPathSegment("revisions");
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("revisionId"));
        urlBuilder.addPathSegment("nodes");
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("nodeId"));
        urlBuilder.addPathSegment("ancestors");

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

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDAncestorNodesRequestProvider.Builder> {
        public abstract ThreeDAncestorNodesRequestProvider build();
    }

}
