## Streaming data from Cognite Data Fusion

In addition to the [common read/write capabilities](readAndWriteData.md) you can also stream data from 
Cognite Data Fusion (CDF). CDF does not (yet) offer a native push interface so the SDK simulates streaming by regularly 
polling CDF for data updates and pushing these to the client. 

You can configure the behavior of the stream if needed:
- _Polling interval_. The polling interval defines how frequent the SDK will poll CDF for updates. The default interval 
is every 5 seconds.
- _Polling offset_. The polling offset is a time window "buffer" subtracted from the current time when polling 
for data from CDF Raw. It is intended as a safeguard for clock differences between the client (running this 
publisher) and the CDF service. For example, if the polling offset is two (2) seconds, then the SDK will look 
"two seconds back in time" when polling for updates.
- _Start time_. The starting time stamp that the SDK will stream from. The default is `UNIX epoch` so all pre-existing 
data objects will be included in the stream. If you want to stream from the current time and forwards, you should 
adjust the start time to `now`.
- _End time_. The ending time for the data stream. The default is to run the stream continuously without a set end time.

### Streaming rows from a Raw table

`Raw` is the first resource type to get stream support in the SDK. You can continuously stream rows from a Raw table 
so that you can implement "streaming" (well, strictly speaking it is micro-batching) data pipelines from `Raw` 
to the Cognite data model (`assets, events, time series, relationships, etc.`). You can also use `Raw` as a durable 
message queue. 

A table in `Raw` is strongly consistent so using it as the basis for streaming data can work well and with low latency.

```java
// Build the client using OpenID Connect client credentials (or API key)
CogniteClient client = CogniteClient.ofClientCredentials(
        <clientId>,
        <clientSecret>,
        TokenUrl.generateAzureAdURL(<azureAdTenantId>))
        .withProject("myCdfProject")
        .withBaseUrl("https://yourBaseURL.cognitedata.com"); //optional parameter

// Set up a basic publisher. This will stream all existing rows pluss all new/changed rows
// at a polling interval of every 5 seconds. 
client.raw().rows().stream(dbName, tableName)               // The raw table to stream from
        .withConsumer(batch -> {                            // Add your "receiver/listener" as a consumer.
                myConsumer.myProcessLogic(batch);           // The publisher will issue one and one batch of 
        })                                                  // rows as a List<RawRow> object to the consumer.
        .start();                                           // start the stream.


// A more advanced example with extra configuration.
RawPublisher publisher = client.raw().rows().stream(dbName, tableName)               // The raw table to stream from
        .withStartTime(Instant.now())                       // Don't read historic data--only include new/changed data from "now"
        .withPollingInterval(Duration.ofSeconds(2))         // Poll for updates every two seconds
        .withConsumer(batch -> {                            // Add your "receiver/listener" as a consumer.
                myConsumer.myProcessLogic(batch);            
        });

Future<Boolean> future = publisher.start();               // The publisher returns a Future when you start the stream.

// If your code needs to get notified when the publisher stops streaming (f. ex. if you have set an end time)
// you can attach to the future
future.get();                                            // will block until the publisher stops streaming.

// You can also ask the publisher to abort the current stream
publisher.abort();

```

