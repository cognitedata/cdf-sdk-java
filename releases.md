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

- Geo location attribute and resource type.

## [0.9.5-SNAPSHOT]

### Fixed

- Populate auth headers for custom CDF hosts.
- Null values in raw row should not raise exceptions. 

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