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
public abstract class ThreeDRequestProvider extends GenericRequestProvider {

    public static ThreeDRequestProvider.Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_ThreeDRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDRequestProvider.Builder toBuilder();

    public ThreeDRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        return toBuilder().setRequest(parameters).build();
    }

    public okhttp3.Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request requestParameters = getRequest();
        okhttp3.Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

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

        if (requestParameters.getRequestParameters().containsKey("published")) {
            urlBuilder.addQueryParameter("published", String.valueOf(requestParameters.getRequestParameters().get("published")));
        }

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDRequestProvider.Builder> {
        public abstract ThreeDRequestProvider build();
    }

}
