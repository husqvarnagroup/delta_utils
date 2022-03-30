# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
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

[Unreleased]: https://github.com/bulv1ne/delta_utilscompare/v0.0.1...HEAD
