# Changelog

## [5.1.0] - 2025-09-16

### Changed

- updated `/import/ips`-type job to move data to internal storage

### Added

- added support for hotfolder-based imports

## [5.0.0] - 2025-09-09

### Changed

- migrated to dcm-common v4
- **Breaking:** migrated to API v7

## [4.5.0] - 2025-08-20

### Added

- added support for submission token

## [4.4.0] - 2025-08-14

### Changed

- migrated to new extension system

### Added

- added optional constructor parameters `max_resumption_tokens` to the oai-plugins and controlled via the app-config

## [4.2.1] - 2025-07-28

### Fixed

- loosened dcm-ip-builder-sdk version range (supports v5)

## [4.2.0] - 2025-07-25

### Added

- added test-import mode (internal and external)
- added second version of oai-plugin to support multiple sets and transferUrlFilters based on xpath

### Fixed

- added logging of an error-message when generating a bad IE with demo-plugin
- fixed initialization of ScalableOrchestrator with ORCHESTRATION_PROCESSES

## [4.0.0] - 2025-02-14

### Changed

- refactored to use plugin system from `dcm-common`
- **Breaking:** migrated to Object Validator API v5
- **Breaking:** migrated to IP Builder API v4
- **Breaking:** migrated to Import Module API v6

## [3.0.2] - 2024-11-28

### Fixed

- fixed copy & paste-error in environment configuration in `README.md`

## [3.0.1] - 2024-11-21

### Changed

- updated package metadata, Dockerfiles, and README

## [3.0.0] - 2024-10-16

### Changed

- **Breaking:** implemented changes of API v5 (`abec9d5a`, `d65abc95`, `c55530a6`)
- migrated to `dcm-common` (scalable orchestration and related components; latest `DataModel`) (`abec9d5a`)

### Added

- added endpoint for importing ips from internal storage (`d65abc95`)

## [2.0.1] - 2024-07-26

### Fixed

- added missing plugin/builder result summary to log (`96cd9d9`)

## [2.0.0] - 2024-07-24

### Changed

- improved report.progress.verbose and log messages (`b320fe7b`, `268df065`, `d14b0e39`)
- **Breaking:** updated to API v4 (`90c4c0e9`, `13a3a3a6`)

### Added

- fixed bad values for `data.success` in intermediate reports (`13a3a3a6`)
- added mechanism for providing plugin-progress (`f2b2272a`, `832a366c`, `139f96c5`)
- added retry-mechanism for source system timeout during harvest (`468fd233`)

## [1.0.0] - 2024-04-25

### Changed

- **Breaking:** renamed app-config class (`cdede5cf`)
- reorganized package structure (`bfda2bae`)
- switched to new sdk for ip builder api v1 (`bfda2bae`)
- **Breaking:** implemented changed import module api v3 (`bfda2bae`, `87152537`, `fe6ed93d`, `38e7acf3`)
- updated version for `lzvnrw_supplements.orchestration` (`2c092317`)
- updated version for `lzvnrw_supplements.logger` (`2c092317`)
- defined new return format from plugins to application (`bd16a350`, `9f3523a9`)
- updated input-handling based on data-plumber-http (`33629db1`)

### Added

- added source system timeout as environment setting (`c8df950d`)
- added extras-dependency for `Flask-CORS` (`b853c1cc`)
- plugin verifies success of payload download (`4982ce8b`)

## [0.1.0] - 2024-02-22

### Changed

- initial release of dcm-import-module
