## Java SDK for Cognite Data Fusion (CDF)

The Java SDK provides convenient access to Cognite Data Fusion's capabilities. It covers a large part of CDF's 
capability surface, including experimental features. In addition, it is designed to handle a lot of the client "chores" 
for you so you can spend more time on your core client logic. 

Some of the SDK's capabilities:
- _Upsert support_. It will automatically handle `create`and `update` for you.
- _Retries with backoff_. Transient failures will automatically be retried.
- _Performance optimization_. The SDK will handle batching and parallelization of requests.

The Java SDK follows the Cognite Data Fusion (CDF) REST API structure. Both in terms of the different endpoints
(`assets`, `events`, `contextualization`, etc.), the operations (`list`, `retrive`, `upsert`, etc.) and the
data transfer objects (`asset`, `event`, etc.). Therefore, the [CDF REST API documentation](https://docs.cognite.com/api/v1/)
is an excellent foundation also for working with the Java SDK. Most of the SDK's features and usage specifications 
map directly to the API's definitions.

### Table of contents

- [Client configuration. How to setup authentication.](clientSetup.md)
- [Reading and writing data. All the basics of how to use the SDK](readAndWriteData.md)
- [Streaming data from Cognite Data Fusion](streamingData.md)
- [The Asset resource type. The secret sauce for handling the asset-hierarchy](assets.md)
- [Time series--TBD]
- [Contextualization](contextualization.md)
