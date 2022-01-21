package com.cognite.client.servicesV1.request;

import com.cognite.client.Request;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.request.serializer.ThreeDNodePropertiesFilterSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class ThreeDNodesFilterRequestProvider extends GenericRequestProvider {

    private ObjectMapper mapper = new ObjectMapper();

    public static ThreeDNodesFilterRequestProvider.Builder builder() {
        return new AutoValue_ThreeDNodesFilterRequestProvider.Builder()
                .setRequest(Request.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract ThreeDNodesFilterRequestProvider.Builder toBuilder();

    public ThreeDNodesFilterRequestProvider withRequest(Request parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
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

        SimpleModule module = new SimpleModule();
        module.addSerializer(Request.class, new ThreeDNodePropertiesFilterSerializer());
        mapper.registerModule(module);

        String outputJson = mapper.writeValueAsString(requestParameters);
        okhttp3.Request.Builder build = requestBuilder.url(urlBuilder.build());

        return build.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<ThreeDNodesFilterRequestProvider.Builder> {
        public abstract ThreeDNodesFilterRequestProvider build();
    }
}
