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

- Geo-location attribute and resource type.

## [1.4.0-SNAPSHOT]

### Added

- Support for `ExtractionPipeline` and `ExtractionPipelineRun` so you can send extractor/pipeline observations and 
 heartbeats to CDF.
- Improved performance of `Relationships.list()` with the added support for partitions.
- Support for including the `source` and `target` object of a `relationship` when using `list()` or `retrieve()`.
- Improved performance of `Sequences.list()` with the added support for partitions.

### Fixed

- More stability improvements to file binary downloads, in particular in situations with limited bandwidth. 
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