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

## [0.9.11-SNAPSHOT]

### Fixed

- More robust file binary download when running very large jobs.

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