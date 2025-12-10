# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

## [0.7.0] - 2025-12-10
### Changed
- Support for pyspark>=4
- Support for python>=3.12
- Build backend to uv_build
- Support for Databricks Runtime 17.3 LTS

## [0.6.0] - 2025-08-22
### Added
- Support for `delta_utils.geocoding.lookup_country` can run on serverless

### Changed
- Using DataFrame.mapInArrow instead of DataFrame.rdd.mapPartitions in `delta_utils.geocoding.lookup_country`
  resulted in an accidental performance increase

## [0.5.0] - 2024-12-18
### Added
- Warning message when initializing a DeltaChanges class when the table is not a delta table
- New geocoding module to resolve country and address for a geo position
- New SSM module to fetch data from AWS parameter store
- Function drop_all_parameters_null_columns
- Function location_for_hive_table
- Lineage class to get downstream_tables
- Upgrade to Python 3.10 and above

### Fixed
- DeltaTable.isDeltaTable doesn't seem to work with Unity Catalog,
  the function `delta_utils.core.last_written_timestamp_for_delta_path` will now try to get the last timestamp
  regardless if the dataset exists or is a delta table, it will return `None` and print an error message
  if it couldn't get the last timestamp

## [0.4.0] - 2022-11-25
### Changed
- Fileregistry is deprecated, with Unity Catalog S3 works entirely different and boto3 is not possible
- DeltaChanges work with Unity Catalog table names

## [0.3.0] - 2022-05-04
### Added
- Github Action changelog.yml to check if the CHANGELOG.md file is being changed in the pull request
- Nested names option for flatten function

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

[Unreleased]: https://github.com/husqvarnagroup/delta_utils/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/husqvarnagroup/delta_utils/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/husqvarnagroup/delta_utils/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/husqvarnagroup/delta_utils/compare/v0.0.1...v0.1.0
