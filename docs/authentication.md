## Authentication

There are two authentication options:
- OpenID Connect.
- API keys.

### OpenID Connect

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
CogniteClient client = CogniteClient.ofClientCredentials(
                    <clientId>,
                    <clientSecret>,
                    TokenUrl.generateAzureAdURL(<azureAdTenantId>));
```

There are also other authentication flows under the OpenID umbrella, but the SDK don't have built-in 
support for these. Instead you can extend the SDK by handling these other flows with external libraries
(or your own code :)). You then link up the SDK as follows:
```java
CogniteClient client = CogniteClient.ofToken(Supplier<String> tokenSupplier);
```
The `tokenSupplier` is a functional interface, so you can pass in a lambda function. The `tokenSupplier`
will be called for each api request and expects a valid token in return. The token is added to
the `Authorization` header in each request. Your supplier needs to produce the entire value for 
the header, including the `Bearer` prefix. That is, your supplier should produce a String
of the following pattern: `Bearer <your-access-token>`.

### API keys

Authentication via API key is the legacy method of authenticating services towards Cognite Data Fusion.
You simply supply the API key when creating the client:
```java
CogniteClient client = CogniteClient.ofKey(<apiKey>);
```