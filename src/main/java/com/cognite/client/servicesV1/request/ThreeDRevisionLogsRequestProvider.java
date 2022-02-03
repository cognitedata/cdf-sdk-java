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
public abstract class ThreeDRevisionLogsRequestProvider extends GenericRequestProvider {

    public static ThreeDRevisionLogsRequestProvider.Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_ThreeDRevisionLogsRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDRevisionLogsRequestProvider.Builder toBuilder();

    public ThreeDRevisionLogsRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("modelId")
                        && parameters.getRequestParameters().get("modelId") instanceof Long,
                "Request parameters must include modelId with a Long value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("revisionId")
                        && parameters.getRequestParameters().get("revisionId") instanceof Long,
                "Request parameters must include revisionId with a Long value");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder.addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("modelId")));
        urlBuilder.addPathSegment("revisions");
        urlBuilder.addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("revisionId")));
        urlBuilder.addPathSegment("logs");

        if (requestParameters.getRequestParameters().containsKey("severity")) {
            urlBuilder.addQueryParameter("severity", String.valueOf(requestParameters.getRequestParameters().get("severity")));
        }

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDRevisionLogsRequestProvider.Builder> {
        public abstract ThreeDRevisionLogsRequestProvider build();
    }

}
