## Reading and writing data from/to Cognite Data Fusion

The Java SDK follows the Cognite Data Fusion (CDF) REST API structure, so it is helpful to familiarize yourself with 
the Cognite API documentation: [https://docs.cognite.com/api/v1/](https://docs.cognite.com/api/v1/). The SDK is 
structured into different sections that mirror the API endpoints (`assets`, `events`, `contextualization`, etc.). These 
sections host the operations you can access (`list`, `retrive`, `upsert`, etc.). 

Most operations will consume and/or return some data objects. The Java SDK uses typed objects based on Protobuf 
([https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers)). The structure 
of the data transfer objects follow the structure of the Cognite API objects. 

### Common data read/write operations

adfgadg

### Data transfer objects (resource types)

aadfgetdg

### Migrating from SDK <0.9.9

Previous to version 0.9.9 the proto objects used a workaround to represent `optional` attributes. 