package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.LoginStatus;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.google.auto.value.AutoValue;

@AutoValue
public abstract class Login extends ApiBase {

    private static Login.Builder builder() {
        return new AutoValue_Login.Builder();
    }

    /**
     * Constructs a new {@link Login} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the login status api object.
     */
    public static Login of(CogniteClient client) {
        return Login.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link LoginStatus} representing login status api endpoints.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      LoginStatus loginStatus = client.login().loginStatusByApiKey();
     * }
     * </pre>
     *
     * @see ConnectorServiceV1#readLoginStatusByApiKey(String)
     * @see CogniteClient
     * @see CogniteClient#login()
     *
     * @return The LoginStatus api endpoints.
     */
    public LoginStatus loginStatusByApiKey() throws Exception {
        return getClient().getConnectorService().readLoginStatusByApiKey(getClient().getBaseUrl());
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Login.Builder> {
        abstract Login build();
    }
}
