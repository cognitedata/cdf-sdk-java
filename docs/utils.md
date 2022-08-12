## Utilities: ease the development of extractors and data pipelines

In addition to the [common read/write capabilities](readAndWriteData.md) you can also stream data from
Cognite Data Fusion (CDF). CDF does not (yet) offer a native push interface so the SDK simulates streaming by regularly
polling CDF for data updates and pushing these to the client. 