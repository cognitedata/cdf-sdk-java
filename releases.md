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

- Native token authentication (OpenID Connect).
- Geo location attribute and resource type.

## [0.9.3-SNAPSHOT]

### Fixed

- Error when trying to download a file binary that only has a file header object in CDF.

## 0.9.2

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