
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.5] - 2021-10-11
### Fixed
- Fixed failing tests after merging fixMissing pull request.

## [2.1.4] - 2021-09-10
### Changed
- Smallplugin-sdk dependency updated to 0.3.0.

## [2.1.3] - 2020-11-11
### Fixed
- Dynamic plugin loading in `Apache Spark` environment.

## [2.1.2] - 2020-10-23
### Removed
- The explicit dependency from the `dispatcher-sdk` module.
### Changed
- Version of the library `smallplugin-sdk` to 0.2.0.

## [2.1.1] - 2020-10-22
### Fixed
- Algorithm specific configuration load failure now throws an error instead of silently creation of empty config.
- The _fit_ command query with an empty target column provides `None` to a model implementation instead of `Some(null)`.
- The service columns (it is starts and ends with double underscore character) drop from the resulting data frame of the _apply_'s command.
### Added
- Test for the _fit_ command.
- Test for the _apply_ command.
- Test for the _score_ command.

## [2.1.0] - 2020-10-16
### Changed
- Algorithms now is separated from this SMaLL core plugin.
- The configuration file plugin.conf has an individual extensions sections for commands _fit_, _apply_, _score_.
- `SavedModel` algorithm of the _apply_ command now is a part of this SMaLL core plugin.
### Added
- Ability to use individual configuration file for each of extensions.
- Ability to set default algorithm implementation for commands _fit_, _apply_, _score_.
- The `configBasePath` parameter of the plugin.conf to set a base path of extensions config files.

## [1.1.1] - 2020-06-04
### Fixed
- Wrong name in plugin.conf. 

## [1.1.0] - 2020-06-03 
### Added
- Makefile.
### Changed
- Remove db section from test application.conf.

## [1.0.0] - 2020-04-03 
### Changed
- Modified to use **Software Development Kit**.

## [0.1.0] - 2020-03-16
### Added
- ML functions _fit_, _apply_, _getdata_, _append_ added.
- Start using "changelog".
