# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Use transactional ddl when applying schema

## [1.3.0] - 2020-12-25

### Added

- Allow custom fields in space format
- Forbid redundant keys in schema top-level and make `spaces` table
  mandatory. So the only valid schema format now is `{spaces = {...}}`.

## [1.2.0] - 2020-07-20

### Added

- Support `uuid` types for tarantool 2.4

## [1.1.0] - 2020-04-09

### Added

- Support `decimal` and `double` types for tarantool 2.3

### Fixed

- Remove unnecessary logs
- Fix error messages

## [1.0.0] - 2019-11-28

### Added

- Basic functionality
- Sharding key support
- Integration tests
- Luarock-based packaging
- Gitlab CI integration
