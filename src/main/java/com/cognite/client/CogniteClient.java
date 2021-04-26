package com.cognite.client;

import com.cognite.client.config.AuthConfig;
import com.cognite.client.dto.LoginStatus;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nimbusds.oauth2.sdk.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the main entry point for interacting with this SDK (and Cognite Data Fusion).
 *
 * All services are exposed via this object.
 *
 * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
 */
@AutoValue
public abstract class CogniteClient implements Serializable {
    private final static String DEFAULT_BASE_URL = "https://api.cognitedata.com";
    private final static String API_ENV_VAR = "COGNITE_API_KEY";

    private static final int DEFAULT_CPU_MULTIPLIER = 8;
    private final static int DEFAULT_MAX_WORKER_THREADS = 8;
    private static ForkJoinPool executorService = new ForkJoinPool(Math.min(
            Runtime.getRuntime().availableProcessors() * DEFAULT_CPU_MULTIPLIER,
            DEFAULT_MAX_WORKER_THREADS));

    protected final static Logger LOG = LoggerFactory.getLogger(CogniteClient.class);

    static {
        LOG.info("CogniteClient - setting up default worker pool with {} workers.",
                executorService.getParallelism());
    }

    @Nullable
    private String cdfProjectCache = null; // Cache attribute for the CDF project

    private static Builder builder() {
        return new AutoValue_CogniteClient.Builder()
                .setClientConfig(ClientConfig.create())
                .setBaseUrl(DEFAULT_BASE_URL);
    }

    /*
    Gather the common config of the http client here. We cannot use a single config entry since we need
    to add specific interceptor depending on the auth method used.
     */
    private static OkHttpClient.Builder getHttpClientBuilder() {
        return new OkHttpClient.Builder()
                .connectTimeout(90, TimeUnit.SECONDS)
                .readTimeout(90, TimeUnit.SECONDS)
                .writeTimeout(90, TimeUnit.SECONDS);
    }

    /**
     * Returns a {@link CogniteClient} using an API key from the system's environment
     * variables (COGNITE_API_KEY) and using default settings.
     * @return the client object.
     * @throws Exception if the api key cannot be read from the system environment.
     */
    public static CogniteClient create() throws Exception {
        String apiKey = System.getenv(API_ENV_VAR);
        if (null == apiKey) {
            String errorMessage = "The environment variable " + API_ENV_VAR + " is not set. Either provide "
                    + "an api key directly to the client or set it via " + API_ENV_VAR;
            throw new Exception(errorMessage);
        }

        return CogniteClient.ofKey(apiKey);
    }

    /**
     * Returns a {@link CogniteClient} using the provided API key for authentication.
     *
     * @param apiKey The Cognite Data Fusion API key to use for authentication.
     * @return the client object with default configuration.
     */
    public static CogniteClient ofKey(String apiKey) {
        Preconditions.checkArgument(null != apiKey && !apiKey.isEmpty(),
                "The api key cannot be empty.");

        return CogniteClient.builder()
                .setHttpClient(CogniteClient.getHttpClientBuilder()
                        .addInterceptor(new ApiKeyInterceptor(apiKey))
                        .build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using the provided bearer token for authorization.
     *
     * If your application handles the authentication flow itself, you can pass the resulting access token
     * to this constructor. The token will be added as a bearer token to all requests.
     *
     * When the token expires you need to create a new client with the refreshed token.
     *
     * @param token The Cognite Data Fusion API key to use for authentication.
     * @return the client object with default configuration.
     */
    public static CogniteClient ofToken(String token) {
        Preconditions.checkArgument(null != token && !token.isEmpty(),
                "The api key cannot be empty.");

        return CogniteClient.builder()
                .setHttpClient(CogniteClient.getHttpClientBuilder()
                        .addInterceptor(new TokenInterceptor(token))
                        .build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using client credentials for authentication.
     *
     * Client credentials is the preferred authentication pattern for services /
     * machine to machine communication for Openid Connect (and Oauth) compatible identity providers.
     *
     * @param clientId The client id to use for authentication.
     * @param clientSecret The client secret to use for authentication.
     * @param tokenUrl The URL to call for obtaining the access token.
     * @return the client object with default configuration.
     */
    public static CogniteClient ofClientCredentials(String clientId,
                                                    String clientSecret,
                                                    URL tokenUrl) {
        Preconditions.checkArgument(null != clientId && !clientId.isEmpty(),
                "The clientId cannot be empty.");
        Preconditions.checkArgument(null != clientSecret && !clientSecret.isEmpty(),
                "The secret cannot be empty.");

        return CogniteClient.builder()
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setTokenUrl(tokenUrl)
                .setHttpClient(CogniteClient.getHttpClientBuilder()
                        .addInterceptor(new ClientCredentialsInterceptor(clientId, clientSecret, tokenUrl, DEFAULT_BASE_URL + "/.default"))
                        .build())
                .build();
    }

    protected abstract Builder toBuilder();
    @Nullable
    protected abstract String getProject();
    @Nullable
    protected abstract String getClientId();
    @Nullable
    protected abstract String getClientSecret();
    @Nullable
    protected abstract URL getTokenUrl();

    protected abstract String getBaseUrl();
    public abstract ClientConfig getClientConfig();
    public abstract OkHttpClient getHttpClient();

    public ForkJoinPool getExecutorService() {
        return executorService;
    }

    /**
     * Returns a {@link CogniteClient} using the specified Cognite Data Fusion project / tenant.
     *
     * @param project The project / tenant to use for interacting with Cognite Data Fusion.
     * @return the client object with the project / tenant key set.
     */
    public CogniteClient withProject(String project) {
        return toBuilder().setProject(project).build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified base URL for issuing API requests.
     *
     * The base URL must follow the format {@code https://<my-host>.cognitedata.com}. The default
     * base URL is {@code https://api.cognitedata.com}
     *
     * @param baseUrl The CDF api base URL
     * @return the client object with the base URL set.
     */
    public CogniteClient withBaseUrl(String baseUrl) {
        Preconditions.checkArgument(null != baseUrl && !baseUrl.isEmpty(),
                "The base URL cannot be empty.");
        return toBuilder().setBaseUrl(baseUrl).build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified configuration settings.
     *
     * @param config The {@link ClientConfig} hosting the client configuration setting.
     * @return the client object with the config applied.
     */
    public CogniteClient withClientConfig(ClientConfig config) {
        // Modify the no threads in the executor service based on the config
        LOG.info("Setting up client with {} worker threads and {} list partitions",
                config.getNoWorkers(),
                config.getNoListPartitions());
        if (config.getNoWorkers() != executorService.getParallelism()) {
            executorService = new ForkJoinPool(config.getNoWorkers());
        }

        return toBuilder().setClientConfig(config).build();
    }

    /**
     * Returns {@link Assets} representing the Cognite assets api endpoint.
     *
     * @return The assets api object.
     */
    public Assets assets() {
        return Assets.of(this);
    }

    /**
     * Returns {@link Timeseries} representing the Cognite timeseries api endpoint.
     *
     * @return The timeseries api object.
     */
    public Timeseries timeseries() {
        return Timeseries.of(this);
    }

    /**
     * Returns {@link Events} representing the Cognite events api endpoint.
     *
     * @return The events api object.
     */
    public Events events() {
        return Events.of(this);
    }

    /**
     * Returns {@link Files} representing the Cognite files api endpoints.
     *
     * @return The labels api endpoint.
     */
    public Files files() {
        return Files.of(this);
    }

    /**
     * Returns {@link Relationships} representing the Cognite relationships api endpoint.
     *
     * @return The relationships api object.
     */
    public Relationships relationships() {
        return Relationships.of(this);
    }

    /**
     * Returns {@link Sequences} representing the Cognite sequences api endpoint.
     *
     * @return The sequences api object.
     */
    public Sequences sequences() {
        return Sequences.of(this);
    }

    /**
     * Returns {@link Raw} representing the Cognite Raw service.
     *
     * @return The raw api object.
     */
    public Raw raw() {
        return Raw.of(this);
    }

    /**
     * Returns {@link Labels} representing the Cognite labels api endpoints.
     *
     * @return The labels api endpoint.
     */
    public Labels labels() {
        return Labels.of(this);
    }

    /**
     * Returns {@link Datasets} representing the Cognite dats sets api endpoint.
     *
     * @return The data sets api object.
     */
    public Datasets datasets() {
        return Datasets.of(this);
    }

    /**
     * Returns {@link SecurityCategories} representing the Cognite labels api endpoints.
     *
     * @return The security categories api endpoint.
     */
    public SecurityCategories securityCategories() {
        return SecurityCategories.of(this);
    }

    /**
     * Returns {@link Contextualization} representing the Cognite contextualization api endpoints.
     *
     * @return The contextualization api endpoint.
     */
    public Contextualization contextualization() {
        return Contextualization.of(this);
    }

    /**
     * Returns {@link Experimental} representing experimental (non-released) api endpoints.
     *
     * @return The Experimental api endpoints.
     */
    public Experimental experimental() {
        return Experimental.of(this);
    }

    /**
     * Returns the services layer mirroring the Cognite Data Fusion API.
     * @return
     */
    protected ConnectorServiceV1 getConnectorService() {
        return ConnectorServiceV1.of(this);
    }

    /**
     * Returns a auth info for api requests
     * @return project config with auth info populated
     * @throws Exception
     */
    protected AuthConfig buildAuthConfig() throws Exception {
        String cdfProject = null;
        if (null != getProject()) {
            // The project is explicitly defined
            cdfProject = getProject();
        } else if (null != cdfProjectCache) {
            // The project info is cached
            cdfProject = cdfProjectCache;
        } else {
            // Have to get the project via the auth credentials
            LoginStatus loginStatus = getConnectorService()
                    .readLoginStatusByApiKey(getBaseUrl());

            if (loginStatus.getProject().isEmpty()) {
                throw new Exception("Could not find the CDF project for the user principal.");
            }
            LOG.debug("CDF project identified for the api key. Project: {}", loginStatus.getProject());
            cdfProjectCache = loginStatus.getProject(); // Cache the result
            cdfProject = loginStatus.getProject();
        }

        return AuthConfig.create()
                .withHost(getBaseUrl())
                .withProject(cdfProject);
    }

    /*
    Interceptor that will add the api key to each request.
     */
    private static class ApiKeyInterceptor implements Interceptor {
        private final String apiKey;

        public ApiKeyInterceptor(String apiKey) {
            Preconditions.checkArgument(null != apiKey && !apiKey.isEmpty(),
                    "The api key cannot be empty.");
            this.apiKey = apiKey;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            okhttp3.Request authRequest = chain.request().newBuilder()
                    .header("api-key", apiKey)
                    .build();

            return chain.proceed(authRequest);
        }
    }

    /*
    Interceptor that will add a bearer token to each request.
     */
    private static class TokenInterceptor implements Interceptor {
        private final String token;

        public TokenInterceptor(String token) {
            Preconditions.checkArgument(null != token && !token.isEmpty(),
                    "The token cannot be empty.");
            this.token = token;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            okhttp3.Request authRequest = chain.request().newBuilder()
                    .header("Authorization", "Bearer " + token)
                    .build();

            return chain.proceed(authRequest);
        }
    }

    /*
    Interceptor that will add a bearer token to each request based on a client credentials
    authentication flow.
     */
    private static class ClientCredentialsInterceptor implements Interceptor {
        private final String clientId;
        private final String clientSecret;
        private final URL tokenUrl;
        private final String scope;

        private String token = null;

        public ClientCredentialsInterceptor(String clientId,
                                            String clientSecret,
                                            URL tokenUrl,
                                            String scope) {
            Preconditions.checkArgument(null != clientId && !clientId.isEmpty(),
                    "The clientId cannot be empty.");
            Preconditions.checkArgument(null != clientSecret && !clientSecret.isEmpty(),
                    "The clientSecret cannot be empty.");
            Preconditions.checkArgument(null != scope && !scope.isEmpty(),
                    "The scope cannot be empty.");
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.tokenUrl = tokenUrl;
            this.scope = scope;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            okhttp3.Request authRequest = chain.request().newBuilder()
                    .header("Authorization", "Bearer " + getToken())
                    .build();

            return chain.proceed(authRequest);
        }

        /*
        Get the access token. If we don't have an access token or if the current token has expired
        we'll have to reach out to the token provider for a new token.
         */
        private String getToken() throws IOException {

            return "";
        }

        /*
        Call the token provider and obtain a new access token.
         */
        private void refreshToken() throws Exception {
            // Construct the client credentials grant
            AuthorizationGrant clientGrant = new ClientCredentialsGrant();

            // The credentials to authenticate the client at the token endpoint
            ClientID clientID = new ClientID(clientId);
            Secret secret = new Secret(clientSecret);
            ClientAuthentication clientAuth = new ClientSecretBasic(clientID, secret);
            Scope tokenScope = new Scope(scope);

            URI tokenEndpoint = tokenUrl.toURI();

            // Make the token request
            TokenRequest request = new TokenRequest(tokenEndpoint, clientAuth, clientGrant, tokenScope);

            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

            if (! response.indicatesSuccess()) {
                // We got an error response...
                TokenErrorResponse errorResponse = response.toErrorResponse();
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();

            // Get the access token
            AccessToken accessToken = successResponse.getTokens().getAccessToken();
        }
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setProject(String value);
        abstract Builder setBaseUrl(String value);
        abstract Builder setClientConfig(ClientConfig value);
        abstract Builder setHttpClient(OkHttpClient value);
        abstract Builder setClientId(String value);
        abstract Builder setClientSecret(String value);
        abstract Builder setTokenUrl(URL value);

        abstract CogniteClient build();
    }
}
