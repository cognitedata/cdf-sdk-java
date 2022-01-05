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
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("modelId"));
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("revisionId"));
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("modelId"));
        urlBuilder.addPathSegment("revisions");
        urlBuilder.addPathSegment((String) requestParameters.getRequestParameters().get("revisionId"));
        urlBuilder.addPathSegment("nodes");

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDNodesRequestProvider.Builder> {
        public abstract ThreeDNodesRequestProvider build();
    }

}
