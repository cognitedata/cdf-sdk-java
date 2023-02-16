# Release log

All notable changes to this project will be documented in this file. 
Changes are grouped as follows:
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon to be removed functionality.
- `Removed` for removed functionality.
- `Fixed` for any bugfixes.
- `Security` in case of vulnerabilities.

## [Planned]

### Medium term

- To be added.

### Short term

## [1.19.0-SNAPSHOT]

### Added

- The new cursor-based `data-points` iterator is enabled by default.
- Support for `geoLocation` on `assets`.
- New versions of `CogniteClient.ofClientCredentials()` and `CogniteClient.ofToken()` which include the `cdfProject` parameter. The `CDF project` must be specified for the client to work correctly.

### Deprecated

- Deprecated `CogniteClient.ofClientCredentials()` and `CogniteClient.ofToken()` which do not include the `cdfProject` parameter. Please migrate to the new versions of these methods which include the `cdfProject` parameter.
- Deprecate `CogniteClient.ofKey()` as the API key authentication method is soon to be removed from Cognite Data Fusion.

### Fixed

- Issue a warning if the `CDF project` is not configured for the client.

## [1.18.0] 2022-10-15

### Added

- Improved performance when reading time series `data point`. When reading 2 - 100 time series in a single request you should see up to 10x improvement in throughput.
- A new cursor-based iteration of time series `data points`. This is pre-release functionality which can be enabled by setting a feature flag in the `ClientConfig`. 
```java
ClientConfig config = ClientConfig.create()
        .withExperimental(FeatureFlag.create()
                .enableDataPointsCursor(true));     // Enable the new cursor-based iterator

CogniteClient.withClientConfig(config);             // Pass the config to the client
```

### Fixed

- The default auth scope breaking when using certain combinations of `CogniteClient.withBaseUrl()` and `CogniteClient.withScopes()`.

## [1.17.0] 2022-09-02

### Added

- Configurable timeout for async api jobs (i.e. `entity matching` and `engineering diagram parsing`). Use `ClientConfig.withAsyncApiJobTimeout(Duration timeout)` to specify a custom timeout. The default timeout is 20 mins.
- Support for configuring a proxy server: [Documentation](../docs/clientSetup.md#proxy-server).
- `UploadQueue` for various resource types to optimize data upload to Cognite Data Fusion: [Documentation](../docs/utils.md).
- `State store` for storing watermark progress states for data applications (extractors, data pipelines, etc.): [Documentation](../docs/utils.md).
- `Extraction pipeline heartbeat` for sending regular `SEEN` status runs to Cognite Data Fusion: [Documentation](../docs/extraction_pipeline.md#create-extraction-pipeline-runs).

### Changed

- Improve javadoc `Assets`
- Improve javadoc `Contextualization`
- Improve javadoc `DataPoints`
- Improve javadoc `Datasets`
- Improve javadoc `EngineeringDiagrams`
- Improve javadoc `EntityMatching`
- Improve javadoc `Events`
- Improve javadoc `Experimental`
- Improve javadoc `ExtractionPipelineRuns`
- Improve javadoc `ExtractionPipelines`
- Improve javadoc `Files`
- Improve javadoc `Labels`
- Improve javadoc `Login`
- Improve javadoc `Raw`
- Improve javadoc `RawDatabases`
- Improve javadoc `RawRows`
- Improve javadoc `RawTables`
- Improve javadoc `Relationships`
- Improve javadoc `Request`
- Improve javadoc `SecurityCategories`
- Improve javadoc `SequenceRows`
- Improve javadoc `Sequences`
- Improve javadoc `ThreeD`
- Improve javadoc `ThreeDAssetMappings`
- Improve javadoc `ThreeDFiles`
- Improve javadoc `ThreeDModels`
- Improve javadoc `ThreeDModelsRevisions`
- Improve javadoc `ThreeDNodes`
- Improve javadoc `ThreeDOutputs`
- Improve javadoc `ThreeDRevisionLogs`
- Improve javadoc `Timeseries`
- Improve javadoc `TransformationJobMetrics`
- Improve javadoc `TransformationJobs`
- Improve javadoc `TransformationNotifications`
- Improve javadoc `Transformation`
- Improve javadoc `TransformationSchedules`

### Fixed

- `list() extractionPipelineRuns` takes `external id` as a required filter parameter. Also fixed CDF api URL used.

## [1.16.0] 2022-05-30

### Added

- Added `Transformation Notifications`
- Geo-location attribute on the `files` resource type is supported.

## [1.15.0] 2022-05-20

### Added

- Added `Transformations`
- Added `Transformation Jobs`
- Added `Transformation Schedules`

### Fixed

- File metadata updates: Fix CDF API payload format


## [1.14.0] 2022-05-19

### Added

- Experimental: Geo-location attribute to files resource type (Geo-location proto-structure is a subject of future changes) 
- `Sequences` upsert support including modified column schema. The `upsert` functionality includes both modified `sequences headers`/`SequenceMetadata` and `sequences rows`/`SequenceBody`. For more information, please refer to the documentation: [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#update-sequences](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#update-sequences) and [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#insert-rows](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#insert-rows)

### Fixed
- File binary upload null pointer exception when running on Android devices.

## [1.13.2] 2022-04-12

### Fixed

- Fix shaded dependencies. Some of the shaded Kotlin libraries caused conflicts when using the SDK from a Kotlin environment.
- Fix duplicated protobuf class files.
- Fix dependency vulnerability. Bump `jackson-databind` to `v2.13.2.1`.

## [1.13.1] 2022-03-22

### Fixed

- File binary download retrying on `SSLException` and `UnknownHostException`. Both may indicate a saturated link for a 
long-running job (which file binary downloads often are).
- Writing `sequence` columns representing integers could cause exceptions.

## [1.13.0] 2022-03-11

### Added

- Files API supports S3 buckets as intermediate store for both read from and write to. 

## [1.12.1] 2022-03-10

### Fixed

- `Files.download()` took a `Path` argument instead of `URI`. 

###

## [1.12.0] 2022-03-09

### Added

- Write string data points. Write requests will chunk strings at 1M UTF8 bytes per request to respect API limits.

### Fixed

- File binary download. Expired URLs not retried properly.

## [1.11.0] 2022-02-18

### Added

- Add utility class `com.cognite.client.util.RawRows` for working with `RawRow` object. Please refer to 
[the documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/raw.md) for more information.

## [1.10.0] 2022-02-02

### Added

- Added `3D Models Revisions`
- Added `3D File Download`
- Added `3D Asset Mapping`
- `EngineeringDiagrams` promoted from experimental to stable. It has the same signature and behavior as before and is 
located under the `contextualization` family: `CogniteClient.contextualization().engineeringDiagrams()`.
- Added convenience methods to the `Request` object for easier handling of items (by `externalId` or `id`). You can use 
`Request.withItemExternalIds(String... externalId)` and `Request.withItemInternalIds(long... internalId)` to add multiple 
items to the request.
- Added convenience methods for retrieving items by `externalId` and `id`: `client.<resourceType>().retrieve(String... externalId)` 
and `client.<resourceType>().retrieve(String... externalId)`. This is implemented by the resource types `Assets`, `DataPoints`, 
`Datasets`, `Events`, `ExtractionPipelines`, `Files`, `Relationships`, `Sequences` and `SequencesRows`.

### Deprecated

- The experimental version of `EngineeringDiagrams` is deprecated given the new, stable version.
- The single item methods `Request.withItemExternalId(String externalId)` and `Request.withItemInternalId(long internalId)` 
have been deprecated in favour of the new multi-item versions.

### Removed

- The old, experimental `pnid` api. This api has been replaced by the `EngineeringDiagrams` api.

## [1.9.0] 2022-01-04

### Added

- Added `3D Models`
- Increased read and write timeouts to match sever-side values

### Fixed

- Upsert of `sequenceMetadata` not identifying duplicate entries correctly.

## [1.8.0] 2021-12-15

### Added

- Experimental streaming support for `events` and `assets`.
- Added `login status` by api-key

### Fixed

- Upsert of `sequenceMetadata` not respecting the max number of cells/columns per batch.

## [1.7.1] 2021-11-23

### Fixed

- Request retries did not work properly in 1.7.0.

## [1.7.0] 2021-11-22

### Fixed

- File binary upload uses PUT instead of POST

## [1.6.3] 2021-11-22

### Fixed

- Improved robustness for file binary upload. Add batch-level retries on a broader set of exceptions.

## [1.6.2] 2021-11-21

### Added

- Improved robustness for file binary upload. Add batch-level retries when the http2 stream is reset. 

## [1.6.1] 2021-11-18

### Fixed

- Parsing error on file binary download using internal `id`.

## [1.6.0] 2021-11-17

### Fixed

- Retry requests on `UnknownHostException`.
- Retry requests on Google Cloud Storage timeouts.

## [1.5.0] 2021-11-11

### Added 

- Support for `dataSetId` in the `Labels` resource type.

### Fixed

- File binary upload robustness.
- `Labels` using api v1 endpoint.

## [1.4.0] 2021-10-31

### Added

- Support for `ExtractionPipeline` and `ExtractionPipelineRun` so you can send extractor/pipeline observations and 
 heartbeats to CDF.
- Improved performance of `Relationships.list()` with added support for partitions.
- Support for including the `source` and `target` object of a `relationship` when using `list()` or `retrieve()`.
- Improved performance of `Sequences.list()` with added support for partitions.

### Fixed

- More stability improvements to file binary downloads, in particular in situations with limited bandwidth. 
- When trying to upload a `File` without a binary, the SDK could throw an exception if the binary was set to 
 be a `URI`.
- `Relationship.sourceType` was wrongly set equal to `Relationship.targetType`.

## [1.3.0] 2021-10-12

### Added

- Support for interacting with a non-encrypted api endpoint (via `CogniteClient.enableHttp(true)`). 
This feature targets testing scenarios. The client will default to secure communication via https which is the only
way to interact with Cognite Data Fusion.

### Fixed
- In some cases `baseUrl` would not be respected when using OIDC authentication.
- Data set upsert fails on duplicate `externalId` / `id`.

## [1.2.0] 2021-09-13

### Changed

- Refactor the experimental `interactive P&ID` to the new `engineering diagram` api endpoint. Basically, 
`client.experimental().pnid().detectAnnotationsPnid()` changes to `client.experimental().engineeringDiagrams().detectAnnotations()`.
Please refer to [the documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/contextualization.md) 
for more information.

### Fixed

- A performance regression introduced in v1.0.0. Performance should be back now :).

## [1.1.1] 2021-08-31

### Fixed

- Streaming reads may not start for very large/high end times.

## [1.1.0] 2021-08-30

### Added

- Streaming support for reading rows from raw tables. [More information in the documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/streamingData.md)
- Support for recursively deleting raw databases and tables.

### Fixed

- `ensureParent` when creating a raw table.

## [1.0.1], 2021-08-18

### Fixed

- Lingering threads could keep the client from shutting down in a timely manner.

## [1.0.0], 2021-08-18

### Fixed

- More robust file binary download when running very large jobs.
- Improved guard against illegal characters in file names when downloading file binaries.

## [0.9.10]

### Added

- Utility methods for converting `Value` to various types. This can be useful when working with CDF.Raw which 
represents its columns as `Struct` and `Value`.
- Ability to synchronize multiple hierarchies via `Assets.synchronizeMultipleHierarchies(Collection<Asset> assetHierarchies)`.
- Utility methods for parsing nested `Struct` and `Value` objects. This can be useful when working with CDF.Raw.

## [0.9.9]

### Added

- Synchronize asset-hierarchy capability. 
- `list()` convenience method that returns all objects for a given resource type.
- User documentation.

### Changed

- Breaking change: Remove the use of wrapper objects from the data transfer objects (`Asset`, `Event`, etc.). Please 
refer to the [documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/readAndWriteData.md#migrating-from-sdk-099) 
  for more information.
- Improved handling of updates for `Relationships`.

### Fixed

- Logback config conflict (issue #37).

## [0.9.8]

### Fixed

- Increased dependencies versions to support Java 11
- Fully automated CD pipeline

## [0.9.7]

### Added

- Labels are properly replaced when running upsert replace for `assets` and `files`.
- Support for recursive delete for `assets`.

## [0.9.6]

### Fixed

- Repeated annotations when generating interactive P&IDs.

## [0.9.5]

### Fixed

- Populate auth headers for custom CDF hosts.
- Null values in raw rows should not raise exceptions. 

## [0.9.4]

### Fixed

- Error when using api key auth in combination with custom host.

## [0.9.3]

### Added

- Support for native token authentication (OpenID Connect).

### Fixed

- Error when trying to download a file binary that only has a file header object in CDF.
- Error when creating new `relationship` objects into data sets.
- Error when uploading file binaries with >1k asset links.

## [0.9.2]

### Added

- Added support for updating / patching `relationship`.

### Fixed

- Fixed duplicates when listing `files`. The list files partition support has been fixed so that you no longer
risk duplicates when manually handling partitions.

## [0.9.1]

### Fixed

- Error when iterating over Raw rows with streams.


## [0.9.0]

### Added

- The initial release of the Java SDK.
