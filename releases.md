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

### Long term

- CDF as a SQL source. Beam SQL cli support.

### Medium term

- Publish job metrics to CDF time series.
- Support 3d nodes.

### Short term

- OOTB incremental read support for time series.
- Separate the generic Java SDK from the Beam specific code (the "Beam Connector").

## [0.9.16-SNAPSHOT]

## [0.9.15]

### Added

- Refactored the core I/O engine into a separate Java SDK. This should also give a general performance improvement of about 2x.

- Support for Beam 2.28.0

### Changed

- Refactored `RequestParameters` from `com.cognite.beam.servicesV1.RequestParameters` to `com.cognite.beam.RequestParameters`.
  All other signatures are the same as before, so you may run a search & replace to update your client.

- Refactored data transfer objects from `com.cognite.beam.io.dto` to `com.cognite.client.dto`. 
  All other signatures are the same as before, so you may run a search & replace to update your client.

- Refactored `com.cognite.beam.io.servicesV1` to `com.cognite.client.servicesV1`. 
  All other signatures are the same as before, so you may run a search & replace to update your client.
  
- Refactored `com.cognite.beam.io.config.UpsertMode` to `com.cognite.client.config.UpsertMode`.
  All other signatures are the same as before, so you may run a search & replace to update your client.
  
- `EntityMatch.matchTo` renamed to `EntityMatch.target` to align with the entity matching api v1.

- Experimental: The interactive P&ID service has been updated to use the new entities specification.
  
### Fixed

- Fixed a bug causing the number of write shards to be double of the configured value.

- Fixed missing duplicate detection when upserting sequences rows. 

- Fixed a bug when reading `Relationship` where `TargetExternalId` would always be set to `null`.

- Fixed a null pointer exception when using GCS temp storage for files--in some cases GCS is unable to report a correct content size.

## 0.9.14

### Fixed

- Update replace sequences with empty metadata.

## 0.9.13

### Added

- Support for `read by id` for `assets`, `events`, `time series`, `sequences` and `files`.
- Support for Beam 2.26.0

### Changed

- Renamed `CogniteIO.readFilesBinariesById` to `CogniteIO.readAllFilesBinariesById` in order to be consistent with the `read` vs. `readAll` pattern.

### Fixed

- Null values in sequences. Sequences now support null values.

## 0.9.12

### Added

- Support for reading and writing `sequences`.  
- Support read `aggregates` for `assets`, `events`, `time series`, `files` and `sequences`.
- Support Beam 2.25.0

## 0.9.11

### Added

- Support for (very) large file binaries via temp blob storage. 
  - Google Cloud Storage and local file storage are supported as temp storage.

### Changed

- New endpoints for entity matcher train and predict.
- New naming for configuration of entity mather and P&ID transforms.

## 0.9.10

### Added

- Relationships in beta.

### Changed

- Entity matcher runs towards the new entity matching endpoints in playground.

## 0.9.9

### Added 

- Improved logging when building interactive P&IDs and when writing files.

## 0.9.8

### Added

- Labels support for files.
- `File.directory` as new attribute on `File`

### Changed

- Create interactive P&ID runs towards the new P&ID api endpoints.
- The create interactive P&ID transform has been refactored with new input type and config options.

### Fixed

- Entity matcher api error messages propagation.
- File writer handles files with >1k asset links.
- File writer handles files with empty file binary.

## 0.9.7

### Added

- New `readDirect` and `writeDirect` modes for time series points. These modes bypass some of the built-in data
validation and optimization steps--this allows for higher performance under some circumstances (for example
if the input data is pre-sorted).

### Changed

- The entity matcher now runs towards the new `fit` and `predict` endpoints.

### Fixed

- Time series writer metrics. 

## 0.9.6

### Added

- Support for entity matching.

### Fixed

- Read and write metrics.

## 0.9.4

### Added

- Generate interactive P&IDs (experimental)
- Experimental support for streaming reads from Raw, Assets, Events and Files.
- Support for security categories on files.
- Support for Labels


## 0.9.3

### Fixed

- Reading from Raw in combination with GCP Secret Manager.

## 0.9.2

### Fixed

- Not fetching secret from GCP Secret Manager.

## 0.9.1

### Added

- Add support for `Relationships`.

### Changed

- `GcpSecretManager` instantiation via `of(projectId, secretId)` instead of `create()`. To highlight the needed input properties.

## 0.9.0

### Added

- Experimental: Streaming support for reading time series datapoints.
- Experimental: Optimized code path for reading time series datapoints.
- Support for GCP secret manager.
- Add datasets as a resource type.
- `Asset` and `AssetLookup` has added support for aggregated properties (`childCount`, `depth` and `path`).
- Add support for the aggregates endpoint for `Asset`, `Event`, `Timeseries`and `File`.

### Changed

- `legacyName` is no longer included when creating time series.
- The `BuildAssetLookup` transform will include aggregated properties in the output `AssetLookup` objects. 
- Support for Beam 2.20.0 

### Fixed

- Data set id included as a comparison attribute for the synchronize assets function.


## 0.8.5

### Fixed

- Upsert items is more robust against race conditions (on large scale upserts) and legacy name collisions.

## 0.8.4

### Added

- Support for data sets for assets, events and time series.

## 0.8.3

### Fixed

- Concurrency error when writing files with more than 2k assetIds.

## 0.8.2

### Added

- Support upsert of files with more than 1k assetIds.

## 0.8.1

### Added

- Utility transform for building read raw table requests based on a config file.

## 0.8.0

### Added

- New request execution framework towards the Cognite API. This enables "fan-out" per worker (multiple parallell async IOs).
- Support for file binaries.
- Support for multi TS read requests. This should speed up wide and shallow queries by 5 - 10x.
- Support for data set for files.

### Fixed

- Default limit for TS points aggregates requests are now set to 10k.

## 0.7.10

### Fixed
- Updated the timeseries point reader to the new proto payload from the Cognite API.
- Read rows from Raw now works in combination with a column specification.

## 0.7.9

### Added

- Add delta read support to TS headers.

### Fixed

- RAW. `minLastUpdatedTime` and `maxLastUpdatedTime`did not work properly in combination with range cursors.

## 0.7.8

### Added

- Files. Support updating `mimeType`.
- CSV reader: Strip out empty lines (lines with no alphabetic characters).

### Fixed

- TS points. Can now resolve duplicates based on `legacyName`.

## 0.7.7

### Added
- CSV reader: Added support for BOM (byte order mark), quoting and custom header.

## 0.7.6

### Added

- Support for reading csv / delimited text files.

## 0.7.5

### Added

- Support for the new TS header advanced filter and partition endpoint.
- Optimized writes for low frequency TS datapoints.

### Fixed

- Api metrics. Fixed batch size mertric when writing timeseries data points.

### Changed

- Changed upper limit on write batch latency.

## 0.7.4

### Added

- Metric for read time series datapoints batch size.
- Utility transforms for parsing TOML config files to map and array.

### Fixed
- `isStep` is added to `TimeseriesPoint`.

## v0.7.3

### Fixed

- Update of time series headers includes `name`.

## v0.7.2

### Fixed

- Delta configuration via parameters and templates.
- `ReadRawRow` configured with `dbName` and/or `tableName` via parameters and templates.

## v0.7.1

### Added

- Metrics can be enabled/disabled via `readerConfig` and `writerConfig`. The default is enabled.

## v0.7.0

### Added

- Delta read support for raw, assets, events and file metadata/header.
- Metrics. Latency and batch size metrics for Cognite API calls.
- API v1 range partitions. Support for API v1 range partitions for assets and events.

### Changed

- Move to Beam 2.16.0

## v0.6.8

### Fixed

- Read single row from raw by row key.

## v0.6.7

### Fixed

- Asset synchronizer: Performance optimization, better delta detection between CDF and source.
- File metadata/header writer. Fixed issue where creating new file metadata could fail.

## v0.6.6

### Fixed

- Asset synchronizer: Fix data integrity check of empty parentExternalId.

## v0.6.5

### Fixed

- Asset synchronizer: Fixed issue with parsing parentExternalId.
- Asset synchronizer: Fixed issue with data validation not handling multiple hierarchies.

## v0.6.4

### Fixed

- Fixed issue with asset input validation where a root node was not identified when the parentExternalId was null (as opposed to an empty string).

## v0.6.3

### Added

- Additional input validation for asset hierarchy synchronization.

## v0.6.2

### Added

- Max write batch size hint for Raw.
- Utility methods for parsing Raw column values into types.
- Upsert and delete file headers/metadata.

## v0.6.1

### Added

- Read TOML files.

## v0.6.0

### Added

- Write and update assets.
- Asset hierarchy synchonization. Continously mirror source, including insert, updates and deletes (for both node metadata and hierarchy structure).
- Support both "partial update" and "replace" as upsert modes.

### Changed

- From ``xxx.builder().build()`` to ``xx.create()`` for user facing config objects and transforms (like ``RequestParameters``, ``ProjectConfig``, etc.).

## v0.5.3

## Fixed

- Fixed issue when writing TS data points and generating a default TS header. In the case of multiple parallel writes there could be collisions. 

## v0.5.2

## Fixed

- Fixed issue when writing TS headers where duplicate ``legacyName`` exists.

## v0.5.1

## Fixed

- Fixed file list / reader error.

## v0.5.0

### Added

- Support auto-create time series headers/metadata for time series where `isString=true`.
- Support for reading file metadata.
- Support for deleting raw rows.

## Changed

- `ReadRows`, the member `lastUpdatedTime` made optional.

### Fixed

- Fixed (another) null pointer exception when using project configuration parameters on the Dataflow runner.

## v0.4.1

### Fixed

- Fixed null pointer exception when using project configuration files.

## v0.4.0

### Added

- Conveniece methods added to ``RequestParameters`` for setting single item ``externalId`` / ``id``.
- Using protobuf for writing and reading time series data points.
- Support for providing ``ProjectConfig`` via file.

### Changed

- New group and artifact ids.
- Beam version: from 2.13.0 to 2.14.0
- ``isString`` is removed from ``TimeseriesPoint``. This field is redundant as the same information can be obtained from ``getValueOneofCase()``.
- ``oneOf`` cases in ``TimeseriesPoint``, ``TimeseriesPointPost`` and ``Item`` has been renamed for increased clarity.

## v0.3.1

### Fixed

- Fix protobuf version conflict.

## v0.3.0

### Added

- Add support for writing and updating time series data points.

## v0.2.9

### Added

- Add support for writing time series meta data.
- Add support for delete time series.
- Add support for delete assets.

### Fixed

- Fix write insert error for objects with existing "id".

## v0.2.8

### Changed

- Increase connection and read timeouts towards the Cognite API to 90 seconds. This is to accomodate reading potentially large response payloads.
- Optimize handling of cursors.

### Fixed

- Fix TS header reader (request provider error).
- Fix support for templates. ``ValueProvider<T>`` is now lazily evaluated.

## v0.2.7

### Added

- Add rootId to assets.
- Add writeShards and maxWriteBatchLatency to Hints.
- Add support for raw cursors on read.
- Add support for listing raw databases and tables.
- Add shortcut methods for raw.dbName and raw.tableName to ``RequestParameters``.
- Add support for specifying raw.dbName and raw.tableName via ``ValueProvider<String>``.

### Changed

- Renamed "splits" to "shards" (Hints object)

### Removed

- Remove depth and path from assets.

## v0.2.6

### Added

- Add batch identifier to writer logs.

### Changed

- Improve the Raw writer and reader.

## v0.2.5

- Refactored the connector configuration.
- Supports API v1.
- Added support for new readers and writers:
  - Readers: Assets, events, TS header and TS datapoints, Raw.
  - Writers: Events and Raw.
- Writers perform automatic upsert.
- Readers support push-down of all filters offered by the API.

## v0.2.1 and earlier

### Added

- Added support for reading time series headers and datapoints.
- New configuration pattern.
- Added support for reading events.
- Configuration of the reader is captured in a configuration object.
