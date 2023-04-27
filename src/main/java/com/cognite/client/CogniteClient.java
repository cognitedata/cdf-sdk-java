package com.cognite.client;

import com.cognite.client.config.AuthConfig;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nimbusds.oauth2.sdk.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    private static int DEFAULT_NO_WORKERS = 8;
    private static int DEFAULT_NO_TS_WORKERS = 16;
    private static ThreadPoolExecutor executorService = new ThreadPoolExecutor(DEFAULT_NO_WORKERS, DEFAULT_NO_WORKERS,
            1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    private static ThreadPoolExecutor tsExecutorService = new ThreadPoolExecutor(DEFAULT_NO_TS_WORKERS, DEFAULT_NO_TS_WORKERS,
            2000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    // Default http client settings
    private final static List<ConnectionSpec> DEFAULT_CONNECTION_SPECS =
            List.of(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS);

    protected final static Logger LOG = LoggerFactory.getLogger(CogniteClient.class);

    static {
        executorService.allowCoreThreadTimeOut(true);
        tsExecutorService.allowCoreThreadTimeOut(true);
        LOG.info("CogniteClient - setting up default worker pool with {} workers.",
                DEFAULT_NO_WORKERS);
    }

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
        /*
        There is an assumption that all CDF resources (endpoints) have 90 seconds read/write timeouts.
        In other words request must be completed within that 90 seconds timeout otherwise availability numbers will be
        breached. If our client drops connection before that, there is no possible way backend teams to detect that.
         */
        return new OkHttpClient.Builder()
                .connectionSpecs(DEFAULT_CONNECTION_SPECS)
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(90, TimeUnit.SECONDS) // CDF has 90 seconds timeout
                .writeTimeout(90, TimeUnit.SECONDS); // CDF has 90 seconds timeout
    }

    /**
     * Returns a {@link CogniteClient} using the provided supplier (function) to provide
     * a bearer token for authorization.
     *
     * If your application handles the authentication flow itself, you can pass a
     * {@link Supplier} to this constructor. The supplier will be called for each api request
     * and the provided token will be added as a bearer token to the request header.
     *
     * @param cdfProject The CDF project to connect to.
     * @param tokenSupplier A Supplier (functional interface) producing a valid access token when called.
     * @return the client object with default configuration.
     */
    public static CogniteClient ofToken(String cdfProject,
                                        Supplier<String> tokenSupplier) {
        Preconditions.checkArgument(null != cdfProject && !cdfProject.isBlank(),
                "The CDF Project cannot be null or blank.");
        Preconditions.checkNotNull(tokenSupplier,
                "The token supplier cannot be null.");

        String host = "";
        try {
            host = new URL(DEFAULT_BASE_URL).getHost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return CogniteClient.builder()
                .setProject(cdfProject)
                .setTokenSupplier(tokenSupplier)
                .setAuthType(AuthType.TOKEN_SUPPLIER)
                .setHttpClient(CogniteClient.getHttpClientBuilder()
                        .addInterceptor(new TokenInterceptor(host, tokenSupplier))
                        .build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using client credentials for authentication.
     *
     * Client credentials is the preferred authentication pattern for services /
     * machine to machine communication for Openid Connect (and Oauth) compatible identity providers.
     *
     * @param cdfProject The CDF project to connect to.
     * @param clientId The client id to use for authentication.
     * @param clientSecret The client secret to use for authentication.
     * @param tokenUrl The URL to call for obtaining the access token.
     * @param scopes The list of scopes to be used for authentication
     * @return the client object with default configuration.
     */
    public static CogniteClient ofClientCredentials(String cdfProject,
                                                    String clientId,
                                                    String clientSecret,
                                                    URL tokenUrl,
                                                    Collection<String> scopes) {
        Preconditions.checkArgument(null != cdfProject && !cdfProject.isBlank(),
                "The CDF Project cannot be null or blank.");
        Preconditions.checkArgument(null != clientId && !clientId.isBlank(),
                "The clientId cannot be empty.");
        Preconditions.checkArgument(null != clientSecret && !clientSecret.isBlank(),
                "The secret cannot be empty.");

        String host = "";
        try {
            host = new URL(DEFAULT_BASE_URL).getHost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return CogniteClient.builder()
                .setProject(cdfProject)
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setTokenUrl(tokenUrl)
                .setAuthType(AuthType.CLIENT_CREDENTIALS)
                .setAuthScopes(scopes)
                .setHttpClient(CogniteClient.getHttpClientBuilder()
                        .addInterceptor(new ClientCredentialsInterceptor(host, clientId,
                                clientSecret, tokenUrl, scopes))
                        .build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using client credentials for authentication.
     *
     * Client credentials is the preferred authentication pattern for services /
     * machine to machine communication for Openid Connect (and Oauth) compatible identity providers.
     *
     * @param cdfProject The CDF project to connect to.
     * @param clientId The client id to use for authentication.
     * @param clientSecret The client secret to use for authentication.
     * @param tokenUrl The URL to call for obtaining the access token.
     * @return the client object with default configuration.
     */
    public static CogniteClient ofClientCredentials(String cdfProject,
                                                    String clientId,
                                                    String clientSecret,
                                                    URL tokenUrl) {
       return CogniteClient.ofClientCredentials(
               cdfProject, clientId, clientSecret, tokenUrl,
               List.of(createScope(DEFAULT_BASE_URL)));
    }

    protected abstract Builder toBuilder();
    protected abstract String getProject();
    @Nullable
    protected abstract String getClientId();
    @Nullable
    protected abstract String getClientSecret();
    @Nullable
    protected abstract URL getTokenUrl();
    @Nullable
    protected abstract Collection<String> getAuthScopes();
    @Nullable
    protected abstract Supplier<String> getTokenSupplier();

    protected abstract AuthType getAuthType();
    protected abstract String getBaseUrl();
    public abstract ClientConfig getClientConfig();
    public abstract OkHttpClient getHttpClient();

    public ExecutorService getExecutorService() {
        return executorService;
    }
    public ExecutorService getTsExecutorService() {
        return tsExecutorService;
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
     * The base URL must follow the format {@code https://<my-host>.cognitedata.com}, with an
     * optional trailing slash. The default base URL is {@code https://api.cognitedata.com}
     *
     * @param baseUrl The CDF api base URL
     * @return the client object with the base URL set.
     */
    public CogniteClient withBaseUrl(String baseUrl) {
        Preconditions.checkArgument(null != baseUrl && !baseUrl.isEmpty(),
                "The base URL cannot be empty.");
        String host = "";
        try {
            host = new URL(baseUrl).getHost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Set the generic part of the configuration
        CogniteClient.Builder returnValueBuilder = toBuilder()
                .setBaseUrl(baseUrl);

        OkHttpClient.Builder httpClientBuilder = getHttpClient().newBuilder();
        List<Interceptor> interceptors = httpClientBuilder.interceptors();
        boolean removed = false;

        // Add the auth specific config
        switch (getAuthType()) {
            case TOKEN_SUPPLIER:
                removed = interceptors.removeIf(interceptor -> interceptor instanceof TokenInterceptor);
                interceptors.add(new TokenInterceptor(host, getTokenSupplier()));
                break;
            case CLIENT_CREDENTIALS:
                String currentDefaultScope = createScope(getBaseUrl());
                String newDefaultScope = createScope(baseUrl);
                Collection<String> scopes = List.of(newDefaultScope); // Fallback scopes
                if (null != getAuthScopes()) {
                    // Iterate the current scopes. If any of them match the "default scope", replace with
                    // an updated default carrying the new host.
                    scopes = getAuthScopes().stream()
                            .map(scope -> {
                                if (scope.equalsIgnoreCase(currentDefaultScope)) {
                                    return newDefaultScope;
                                } else {
                                    return scope;
                                }
                            })
                            .collect(Collectors.toList());
                }
                returnValueBuilder.setAuthScopes(scopes);
                removed = interceptors.removeIf(interceptor -> interceptor instanceof ClientCredentialsInterceptor);
                interceptors.add(new ClientCredentialsInterceptor(host, getClientId(),
                        getClientSecret(), getTokenUrl(), scopes));
                break;
            default:
                // This should never execute...
                throw new RuntimeException("Unknown authentication type. Cannot configure the client.");
        }
        if (!removed) {
            LOG.warn("Unable to delete the old authentication config. New auth config may not work as expected.");
        }

        return returnValueBuilder
                .setHttpClient(httpClientBuilder.build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified list of scopes for issuing API requests.
     *
     * @param scopes The collection of scopes to be used for OAuth2.0 authentication
     * @return the client object with the authentication handler configured
     */
    public CogniteClient withScopes(Collection<String> scopes) {
        Preconditions.checkArgument(getAuthType() == AuthType.CLIENT_CREDENTIALS,
                "Scopes supported for client credentials mode only.");
        String host;

        try {
            host = new URL(getBaseUrl()).getHost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        OkHttpClient.Builder httpClientBuilder = getHttpClient().newBuilder();
        List<Interceptor> interceptors = httpClientBuilder.interceptors();
        boolean removed = interceptors.removeIf(interceptor -> interceptor instanceof ClientCredentialsInterceptor);
        if (!removed) {
            LOG.warn("Unable to delete the old authentication config. New auth config may not work as expected.");
        }
        interceptors.add(new ClientCredentialsInterceptor(host, getClientId(),
                getClientSecret(), getTokenUrl(), scopes));

        return toBuilder()
                .setAuthScopes(scopes)
                .setHttpClient(httpClientBuilder.build())
                .build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified configuration settings.
     *
     * @param config The {@link ClientConfig} hosting the client configuration setting.
     * @return the client object with the config applied.
     */
    public CogniteClient withClientConfig(ClientConfig config) {
        if (config.getNoWorkers() != DEFAULT_NO_WORKERS) {
            // Modify the no threads in the executor service based on the config
            LOG.info("Setting up client with {} worker threads and {} list partitions",
                    config.getNoWorkers(),
                    config.getNoListPartitions());
            DEFAULT_NO_WORKERS = config.getNoWorkers();
            executorService = new ThreadPoolExecutor(DEFAULT_NO_WORKERS, DEFAULT_NO_WORKERS,
                    1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            executorService.allowCoreThreadTimeOut(true);
        }

        if (config.getNoTsWorkers() != DEFAULT_NO_TS_WORKERS) {
            // Modify the no threads in the executor service based on the config
            LOG.info("Setting up client with {} TS worker threads and {} list partitions",
                    config.getNoWorkers(),
                    config.getNoListPartitions());
            DEFAULT_NO_TS_WORKERS = config.getNoTsWorkers();
            tsExecutorService = new ThreadPoolExecutor(DEFAULT_NO_TS_WORKERS, DEFAULT_NO_TS_WORKERS,
                    2000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            tsExecutorService.allowCoreThreadTimeOut(true);
        }

        CogniteClient.Builder returnValueBuilder = toBuilder();

        // If a proxy has been configured, add its config to the http client.
        if (null != config.getProxyConfig()) {
            OkHttpClient.Builder builder = getHttpClient().newBuilder()
                    .proxy(config.getProxyConfig().getProxy());
            if (null != config.getProxyConfig().getUsername() && null != config.getProxyConfig().getPassword()) {
                Authenticator authenticator = new Authenticator() {
                    @Override
                    public okhttp3.Request authenticate(Route route, okhttp3.Response response) {
                        String credential = Credentials.basic(config.getProxyConfig().getUsername(),
                                config.getProxyConfig().getPassword());
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    }
                };
                builder.proxyAuthenticator(authenticator);
            }
            returnValueBuilder.setHttpClient(builder.build());
        }

        return returnValueBuilder.setClientConfig(config).build();
    }

    /**
     * Enable (or disable) support for http. Set to {@code true} to enable support for http calls. Set to
     * {@code false} to disable support for http (then only https will be possible).
     *
     * The default setting is {@code disabled}. I.e. only https calls are allowed.
     * @param enable Set to {@code true} to enable support for http calls. Set to {@code false} to disable support for http.
     * @return the client object with the config applied.
     */
    public CogniteClient enableHttp(boolean enable) {
        List<ConnectionSpec> connectionSpecs = new ArrayList<>(DEFAULT_CONNECTION_SPECS);
        if (enable) {
            connectionSpecs.add(ConnectionSpec.CLEARTEXT);
        }

        OkHttpClient newClient = getHttpClient().newBuilder()
                .connectionSpecs(connectionSpecs)
                .build();

        return toBuilder().setHttpClient(newClient).build();
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
     * Returns {@link ExtractionPipelines} representing the Cognite extraction pipelines api endpoint.
     *
     * @return The extraction pipelines api object.
     */
    public ExtractionPipelines extractionPipelines() {
        return ExtractionPipelines.of(this);
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
     * Returns {@link ThreeD} representing 3D api endpoints.
     *
     * @return The ThreeD api endpoints.
     */
    public ThreeD threeD() {
        return ThreeD.of(this);
    }

    /**
     * Returns {@link Transformations} representing Transformation api endpoints.
     *
     * @return The Transformation api endpoints.
     */
    public Transformations transformations() {
        return Transformations.of(this);
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
        return AuthConfig.of(getProject())
                .withHost(getBaseUrl());
    }

    private static String createScope(String baseUrl) {
        final String separator = baseUrl.endsWith("/") ? "" : "/";
        return baseUrl + separator + ".default";
    }

    /*
    Interceptor that will add a bearer token to each request. The token is produced by a supplier
    function (functional interface / lambda)
     */
    private static class TokenInterceptor implements Interceptor {
        private final Supplier<String> tokenSupplier;
        private final String apiHost;

        public TokenInterceptor(String host, Supplier<String> tokenSupplier) {
            Preconditions.checkNotNull(tokenSupplier,
                    "The token supplier cannot be empty.");
            this.apiHost = host;
            this.tokenSupplier = tokenSupplier;
        }

        @Override
        public okhttp3.Response intercept(Chain chain) throws IOException {
            if (chain.request().url().host().equalsIgnoreCase(apiHost)) {
                // Only add auth info to requests towards the cognite api host.
                String token = tokenSupplier.get();

                okhttp3.Request authRequest = chain.request().newBuilder()
                        .header("Authorization", token)
                        .build();

                return chain.proceed(authRequest);
            } else {
                return chain.proceed(chain.request());
            }
        }
    }

    /*
    Interceptor that will add a bearer token to each request based on a client credentials
    authentication flow.
     */
    private static class ClientCredentialsInterceptor implements Interceptor {
        private final static String loggingPrefix = "Authentication - ";
        // Refresh the access token 30 secs before it expires
        private final static Duration tokenRefreshGraceDuration = Duration.ofSeconds(30);

        private final String apiHost;

        // The credentials used for acquiring access tokens
        private final String clientId;
        private final String clientSecret;
        private final URL tokenUrl;
        private final Collection<String> scopes;

        // The access token--fetched from the token provider
        private String token = null;
        private long tokenLifetime = 0;  // lifetime in seconds
        private Instant tokenInstant = null;

        public ClientCredentialsInterceptor(String host,
                                            String clientId,
                                            String clientSecret,
                                            URL tokenUrl,
                                            Collection<String> scopes) {
            Preconditions.checkArgument(null != clientId && !clientId.isEmpty(),
                    "The clientId cannot be empty.");
            Preconditions.checkArgument(null != clientSecret && !clientSecret.isEmpty(),
                    "The clientSecret cannot be empty.");
            Preconditions.checkArgument(null != scopes && !scopes.isEmpty(),
                    "The scopes collection cannot be empty.");
            Preconditions.checkState(scopes.stream().noneMatch(Strings::isNullOrEmpty),
                    "The scope cannot be empty.");

            this.apiHost = host;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.tokenUrl = tokenUrl;
            this.scopes = scopes;
        }

        @Override
        public okhttp3.Response intercept(Chain chain) throws IOException {
            if (chain.request().url().host().equalsIgnoreCase(apiHost)) {
                // Only add auth info to requests towards the cognite api host.

                okhttp3.Request authRequest = chain.request().newBuilder()
                        .header("Authorization", getToken())
                        .build();

                return chain.proceed(authRequest);
            } else {
                return chain.proceed(chain.request());
            }
        }

        /*
        Get the access token. If we don't have an access token or if the current token has expired
        we'll have to reach out to the token provider for a new token.
         */
        private String getToken() throws IOException {
            if (null == token
                    || (tokenLifetime > 0 && tokenInstant.plusSeconds(tokenLifetime)
                            .minus(tokenRefreshGraceDuration)
                            .isBefore(Instant.now()))) {
                // The token does not exist or it is too old.
                try {
                    refreshToken();
                } catch (Exception e) {
                    LOG.warn(loggingPrefix + "Refreshing the access token failed: {}",
                            e.toString());
                    throw new IOException(e);
                }
            }

            return token;
        }

        /*
        Call the token provider and obtain a new access token.
         */
        private synchronized void refreshToken() throws Exception {
            Instant startInstant = Instant.now();
            LOG.debug(loggingPrefix + "start access token refresh.");

            // Construct the client credentials grant
            AuthorizationGrant clientGrant = new ClientCredentialsGrant();

            // The credentials to authenticate the client at the token endpoint
            ClientID clientID = new ClientID(clientId);
            Secret secret = new Secret(clientSecret);
            ClientAuthentication clientAuth = new ClientSecretBasic(clientID, secret);
            Scope tokenScope = new Scope(scopes.toArray(new String[0]));

            URI tokenEndpoint = tokenUrl.toURI();

            // Make the token request
            TokenRequest request = new TokenRequest(tokenEndpoint, clientAuth, clientGrant, tokenScope);
            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

            if (! response.indicatesSuccess()) {
                // We got an error response...
                TokenErrorResponse errorResponse = response.toErrorResponse();
                LOG.warn(loggingPrefix + "Unable to get a new access token from the identity provider: {}",
                        errorResponse.toJSONObject().toJSONString());
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();

            // Get the access token
            AccessToken accessToken = successResponse.getTokens().getAccessToken();
            token = accessToken.toAuthorizationHeader();
            tokenInstant = Instant.now();
            tokenLifetime = accessToken.getLifetime();

            LOG.debug(loggingPrefix + "finished access token refresh within duration: {}.",
                    Duration.between(startInstant, Instant.now()).toString());
        }
    }

    /*
    The set of valid authentication types supported by the client.
     */
    protected enum AuthType {
        CLIENT_CREDENTIALS,
        TOKEN_SUPPLIER
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
        abstract Builder setAuthScopes(Collection<String> value);
        abstract Builder setTokenSupplier(Supplier<String> supplier);
        abstract Builder setAuthType(AuthType value);

        abstract CogniteClient build();
    }
}
