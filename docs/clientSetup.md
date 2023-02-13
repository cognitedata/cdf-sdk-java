## Configure the client

The `CogniteClient` is the entry point to all operations towards Cognite Data Fusion (CDF). The client object represents 
an authenticated connection to a specific CDF project. If you operate towards multiple CDF projects, you need to 
create a separate `CogniteClient` for each of them. 

Configuration:
- [Authentication](#authentication)
- [Proxy server](#proxy-server)

### Authentication

There are two authentication options:
- OpenID Connect.
- API keys.

#### OpenID Connect

OpenID Connect tokens are access tokens provided by an identity provider (like Azure AD, Octa, etc.). This token 
is then used to access Cognite Data Fusion. The flow can be as follows:
1) The client authenticates towards the identity provider and obtains an access token.
2) The client uses the access token from 1) when interacting with Cognite Data Fusion.

The SDK has built-in support for the client credentials authentication flow for native tokens. This is the 
default flow for services (machine to machine) like extractors, data applications (transformations, etc.). 

In order to use this flow, you need to register your client with the identity provider. In the case of Azure AD 
this would typically be registering your client as an "app registration". Then use the client credentials (sourced 
from Azure AD) as inputs to the SDK:
```java
String clientId = "my-client-id";
String clientSecret = "my-client-secret";   // Remember to source this value from a secure transfer mechanism.
                                            // For example, via secret mapped to an environment variable.
String azureAdTenantId = "my-aad-tenant-id";
String cdfProject = "my-cdf-project";

CogniteClient client = CogniteClient.ofClientCredentials(
        cdfProject,
        clientId, 
        clientSecret, 
        TokenUrl.generateAzureAdURL(azureAdTenantId);

// Using custom authentication scopes
String cdfBaseUrl = "https://api.cognitedata.com"
List<String> authScopes = List.of(cdfBaseUrl + "/.default", "my-second-scope");

CogniteClient client = CogniteClient.ofClientCredentials(
        cdfProject,
        clientId,
        clientSecret,
        TokenUrl.generateAzureAdURL(azureAdTenantId),
        authScopes)
        .withProject(cdfProject)
        .withBaseUrl(cdfBaseUrl);
```

There are also other authentication flows under the OpenID umbrella, but the SDK don't have built-in 
support for these. Instead you can extend the SDK by handling these other flows with external libraries
(or your own code :)). You then link up the SDK as follows:
```java
String cdfProject = "my-cdf-project";

CogniteClient client = CogniteClient.ofToken(cdfProject, Supplier<String> tokenSupplier);
```
The `tokenSupplier` is a functional interface, so you can pass in a lambda function. The `tokenSupplier`
will be called for each api request and expects a valid token in return. The token is added to
the `Authorization` header in each request. Your supplier needs to produce the entire value for 
the header, including the `Bearer` prefix. That is, your supplier should produce a String
of the following pattern: `Bearer <your-access-token>`.

#### API keys (deprecated)

Authentication via API key is the legacy method of authenticating services towards Cognite Data Fusion.
You simply supply the API key when creating the client:
```java
String cdfProject = "my-cdf-project";
String apiKey = "my-api-key";       // Remember to source this value from a secure transfer mechanism.
                                    // For example, via secret mapped to an environment variable.
CogniteClient client = CogniteClient.ofKey(apiKey)
        .withProject(cdfProject);
```

### Proxy server

If your environment uses a proxy server to connect to the Internet (and send requests to Cognite Data Fusion), you can configure the `CogniteClient` to use your proxy server. You add the proxy server specification to the `ClientConfig` object and pass it to the client.
```java
/* 
HTTP proxy without authentication
 */
// The proxy configuration parameters
String proxyHost = "proxyHost";
int proxyPort = 8080;
// Standard auth config
String clientId = "my-client-id";
String clientSecret = "my-client-secret";
String azureAdTenantId = "my-aad-tenant-id";
String cdfProject = "my-cdf-project";

// Build the Cognite Client with the proxy config
ProxyConfig proxyConfig = ProxyConfig.of(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)));
ClientConfig clientConfig = ClientConfig.create()
        .withProxyConfig(proxyConfig);

CogniteClient client = CogniteClient.ofClientCredentials(
        cdfProject,
        clientId,
        clientSecret,
        TokenUrl.generateAzureAdURL(azureAdTenantId))
        .withClientConfig(clientConfig);

/*
HTTP proxy with authentication
 */
// Add username and password to the ProxyConfig object. Pass the ProxyConfig to ClientConfig as shown above
String proxyHost = "proxyHost";
int proxyPort = 8080;
final String proxyUser = "username";
final String proxyPass = "password";

ProxyConfig proxyConfig = ProxyConfig.of(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)))
        .withUsername(proxyUser)
        .withPasswork(proxyPass);
ClientConfig clientConfig = ClientConfig.create()
        .withProxyConfig(proxyConfig);
```