## Utilities: ease the development of extractors and data pipelines

The SDK hosts a set of utilities designed to make it easier for you to author extractors and/or data pipelines. The utilities resemble the Python SDK `extractor utils`.

Utilities:
- Upload Queue
- State store

### Upload Queue

The UploadQueue batches together items and uploads them to Cognite Data Fusion (CDF). This helps improve performance and minimize the load on the CDF API.

The queue is uploaded to CDF on three conditions:
1) When the queue is 80% full.
2) At a set interval (default is every 10 seconds).
3) When the `upload()` method is called.

The queue is always uploaded when 80% full. This happens on a background thread so your client can keep putting items on the queue while the upload runs in the background. The queue's capacity will help absorb spikes in the data flow and smooth them out before the CDF upload. However, you may find that your client put new items on the queue at a faster rate than the queue uploads--if this happens over time, the queue will start throttling your client by blocking the `put()` to allow the queue to drain.

```java
// Create a queue by calling uploadQueue() from the main api entry points
UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue();

UploadQueue<TimeseriesPointPost, TimeseriesPointPost> dataPointsUploadQueue = client.timeseries().dataPoints().uploadQueue();

// The queue allows you to put items on it wihout blocking the calling thread--the items are uploaded to CDF 
// in the background. You should add call-back functions so you can keep track of successful uploads and
// capture exceptions in case of upload errors.
UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue()
        .withPostUploadFunction(events -> LOG.info("postUploadFunction triggered. Uploaded {} items", events.size()))
        .withExceptionHandlerFunction(exception -> LOG.warn("exceptionHandlerFunction triggered: {}", exception.getMessage()));
```

To manually upload the contents of the queue, call the `upload()` method:
```java
try {
    // Please note that calling this method will block until all items in the queue have been uploaded
    List<Event> uploadResults = eventUploadQueue.upload();
} catch (Exception e) {
    // In case an error happens during upload to CDF, upload() will throw an exception
}
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

// Call stop() when you are finished putting items on the queue. This will stop the background timer, 
// upload any remaining items in the queue and shut down the background threads. This allows the JVM to 
// shut down cleanly when your code finishes executing.
eventUploadQueue.stop();

// You can also use the queue in a try-with-resources statement to ensure automatic resource clean-up.
// No need to call stop() explicitly as this automatically happens when the try statement finishes.
final UploadQueue<Event, Event> eventUploadQueue = client.events().uploadQueue(); // the queue variable must be final 
eventUploadQueue.start()
        
try (eventUploadQueue) {
    while (my-client-has-work-to-do) {
        eventUploadQueue.put(my-event);
    }
} catch (Exception e) {
    
}
```

### State store

The `StateStore` helps keep track of the extraction/processing state of a data application (extractor, data pipeline, contextualization pipeline, etc.). It is designed to keep track of watermarks to enable incremental load patterns.

At the beginning of a run the data application typically calls the `load()` method, which loads the states from the remote store (which can either be a local JSON file or a table in CDF RAW), and during and/or at the end of a run, the `commit()` method is called, which saves the current states to the persistent store.

You can choose the preferred backing storage for your `StateStore`:
- `LocalStateStore` persists the state in a local .json file.
- `RawStateStore` persists the state in a Cognite Data Fusion Raw table.
- `MemoryStateStore` does not persist state but only holds the current state in-memory.

```java
// A state store backed by a local .json file
StateStore stateStore = LocalStateStore.of("./stateStore.json");
stateStore.load();      // load any previously persisted state

// A state store backed by CDF Raw
StateStore stateStore = RawStateStore.of(CogniteClient, "raw-db-name", "raw-table-name";
stateStore.load();      // load any previously persisted state

// An in-memory state store. load() and commit() does not have any effect for this store.
StateStore stateStore = MemoryStateStore.create();
```

You can now use the state store to get states:
```java
// Get the high watermark for a given key/id
OptionalLong highWatermark = stateStore.getHigh("my-key");

// Get the low watermark for a given key/id
OptionalLong lowWatermark = stateStore.getLow("my-key");

// The optionals will be empty if there is no state for the given key/id
if (highWatermark.isPresent()) {
    // your logic
}

// The optionals can also be used in a more functional programming style
getLow("my-key").ifPresent(watermark -> my-watermark-logic);

getLow(key).ifPresentOrElse(
        currentValue -> my-watermark-logic,
        () -> no-watermark-logic
        );
```

You can set states:
```java
stateStore.setHigh("my-key", 123456789);
```
The `setHigh()` / `setLow()` methods will always overwrite the current state. Some times you might want to only set state if larger than the previous state (for high watermarks), in that case consider `expandHigh()` / `expandLow()`:
```java
stateStore.expandHigh(key, highWatermark);
stateStore.expandLow(key, lowWatermark);
```

To store the state to the remote store, use the `commit()` method:
```java
stateStore.commit();
```

You can set the state store to automatically commit state at regular intervals. In this case, use the `stert()` and `stop()` methods:
```java
/ A state store backed by a local .json file
StateStore stateStore = LocalStateStore.of("./stateStore.json");
stateStore.load();      // load any previously persisted state

// Start the commit background thread. The default commit interval is every 20 seconds
stateStore.start();


// Stop the background thread. You should always call this before exit for proper cleanup.
// This will also call commit() one last time.
stateStore.stop();
```