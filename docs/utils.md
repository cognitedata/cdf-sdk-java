## Utilities: ease the development of extractors and data pipelines

The SDK hosts a set of utilities designed to make it easier for you to author extractors and/or data pipelines. The utilities resemble the Python SDK `extractor utils`.

- Upload Queue

### Upload Queue

The UploadQueue batches together items and uploads them to Cognite Data Fusion (CDF). This helps improve performance and minimize the load on the CDF API.

The queue is uploaded to CDF on three conditions:
1) When the queue is 80% full.
2) At a set interval (default is every 10 seconds).
3) When the `upload()` method is called.



In addition to the [common read/write capabilities](readAndWriteData.md) you can also stream data from
Cognite Data Fusion (CDF). CDF does not (yet) offer a native push interface so the SDK simulates streaming by regularly
polling CDF for data updates and pushing these to the client. 