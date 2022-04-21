# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Added
- Github Action changelog.yml to check if the CHANGELOG.md file is being changed in the pull request

## [0.2.1] - 2022-04-21
### Added
- `delta_utils.clean.flatten` to flatten dataframe
- `delta_utils.clean.fix_invalid_column_names` to remove invalid char in column names

## [0.2.0] - 2022-04-20
### Fixed
- Force readthedocs to use mkdocs>=1.3.0

## [0.1.1] - 2022-03-31
### Added
- `delta_utils.fileregistry.S3FullScan.remove_file_paths` to delete rows in the File Registry

### Changed
- `delta_utils.fileregistry.S3FullScan.clear` is renamed to `clear_dates`

## [0.1.0] - 2022-03-30
### Added
- `delta_utils.core.spark_current_timestamp` function to return the spark server timestamp (resolves race conditions)
- `delta_utils.fileregistry.S3FullScan` class to scan S3 bucket + prefix and suffix, this will keep you from loading processed files

### Changed
- `delta_utils.core.read_change_feed` will check if delta.enableChangeDataFeed set to true, otherwise it
raises `ReadChangeFeedDisabled` exception
    - `delta_utils.utils.DeltaChanges` and `delta_utils.utils.NonDeltaLastWrittenTimestamp` will also raise this exception

## [0.0.1] - 2022-03-13
### Added
- First working code
- Tests
- Documentation

[Unreleased]: https://github.com/bulv1ne/delta_utilscompare/v0.2.1...HEAD
[0.2.1]: https://github.com/bulv1ne/delta_utilscompare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/bulv1ne/delta_utilscompare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/bulv1ne/delta_utilscompare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/bulv1ne/delta_utilscompare/v0.0.1...v0.1.0
