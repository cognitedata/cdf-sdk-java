## Utilities: ease the development of extractors and data pipelines

The SDK hosts a set of utilities designed to make it easier for you to author extractors and/or data pipelines. The utilities resemble the Python SDK `extractor utils`.

- Upload Queue

### Upload Queue

The UploadQueue batches together items and uploads them to Cognite Data Fusion (CDF). This helps improve performance and minimize the load on the CDF API.

The queue is uploaded to CDF on three conditions:
1) When the queue is 80% full.
2) At a set interval (default is every 10 seconds).
3) When the `upload()` method is called.

The queue is always uploaded when 80% full. This happens on a background thread so your client can keep putting items on the queue while the upload runs in the background. 

```java
// create a queue by calling uploadQueue() from the main api entry points
UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue();

UploadQueue<TimeseriesPointPost, TimeseriesPointPost> dataPointsUploadQueue = client.timeseries().dataPoints().uploadQueue();

        .withPostUploadFunction(events -> LOG.info("postUploadFunction triggered. Uploaded {} items", events.size()))
        .withExceptionHandlerFunction(exception -> LOG.warn("exceptionHandlerFunction triggered: {}", exception.getMessage()));
```

To manually upload the contents of the queue, call the `upload()` method:
```java
uploadQueue.upload();
```

A common pattern is to have the upload queue start a background timer to upload the queue at regular intervals. This ensures that the uploads are triggered both by maximum batch sizes (at the 80% queue fill rate) and at minimum time intervals.
```java
// create a queue by calling uploadQueue() from the main api entry points
UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue();

// Start the background timer job. The default timer interval is 10 seconds. Remember to call stop() when you are
// finished publishing items.
eventUploadQueue.start();

// publish your data items on the queue for upload
while (my-client-has-work-to-do) {
    eventUploadQueue.put(my-event);
}

// When calling stop(), the background timer is stopped, and the entire queue is drained.
// You should always close the queue after finishing writing to it.
eventUploadQueue.stop();

// You can also use the queue in a try-with-resources statement to ensure automatic resource clean-up
final UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue(); // the queue variable must be final 
try (eventUploadQueue) {
    while (my-client-has-work-to-do) {
        eventUploadQueue.put(my-event);
    }
} catch (Exception e) {
    
}
```


The queue's capacity will help absorb spikes in the data flow and smooth them out before the CDF upload. However, you may find that your client put new items on the queue at a faster rate than the queue uploads--if this happens over time, the queue will start throttling your client by blocking the `put()` to allow the queue to drain.



In addition to the [common read/write capabilities](readAndWriteData.md) you can also stream data from
Cognite Data Fusion (CDF). CDF does not (yet) offer a native push interface so the SDK simulates streaming by regularly
polling CDF for data updates and pushing these to the client. 