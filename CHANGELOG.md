
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Fixed
- Algorithm specific configuration load failure now throws an error instead of silently creation of empty config.
- The _fit_ command query with an empty target column provides `None` to a model implementation instead of `Some(null)`.
### Added
- Test for the _fit_ command.

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
