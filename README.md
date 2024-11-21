# Digital Curation Manager - Import Module

The 'DCM Import Module'-API provides functionality to import
* Intellectual Entities (IEs) from a remote system and
* pre-built Information Packages (IPs) from the internal storage.

This repository contains the corresponding Flask app definition.
For the associated OpenAPI-document, please refer to the sibling package [`dcm-import-module-api`](https://github.com/lzv-nrw/dcm-import-module-api).

The contents of this repository are part of the [`Digital Curation Manager`](https://github.com/lzv-nrw/digital-curation-manager).

## Local install
Make sure to include the extra-index-url `https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple` in your [pip-configuration](https://pip.pypa.io/en/stable/cli/pip_install/#finding-packages) to enable an automated install of all dependencies.
Using a virtual environment is recommended.

1. Install with
   ```
   pip install .
   ```
1. Configure service environment to fit your needs ([see here](#environmentconfiguration)).
1. Run app as
   ```
   flask run --port=8080
   ```
1. To manually use the API, either run command line tools like `curl` as, e.g.,
   ```
   curl -X 'POST' \
     'http://localhost:8080/import/external' \
     -H 'accept: application/json' \
     -H 'Content-Type: application/json' \
     -d '{
     "import": {
       "plugin": "string",
       "args": {
         "metadata_prefix": "oai_dc"
       }
     },
     "build": {
       "configuration": "<base64-string of serialized python-object>",
       "BagItProfile": "https://www.lzv.nrw/bagit_profile_v1.0.0.json",
       "BagItPayloadProfile": "https://www.lzv.nrw/bagit_profile_v1.0.0.json"
     },
     "validation": {
       "modules": [
         "bagit_profile",
         "payload_structure",
         "payload_integrity",
         "file_format"
       ],
       "args": {
         "bagit_profile": {
           "baginfoTagCaseSensitive": true,
           "profileUrl": "bagit_profiles/dcm_bagit_profile_v1.0.0.json"
         },
         "payload_structure": {
           "profileUrl": "bagit_profiles/dcm_bagit_profile_v1.0.0.json"
         }
       }
     }
   }'
   ```
   or run a gui-application, like Swagger UI, based on the OpenAPI-document provided in the sibling package [`dcm-import-module-api`](https://github.com/lzv-nrw/dcm-import-module-api).

## Run with docker compose
Simply run
```
docker compose up
```
By default, the app listens on port 8080.
The docker volume `file_storage` is automatically created and data will be written in `/file_storage`.
To rebuild an already existing image, run `docker compose build`.

Additionally, a Swagger UI is hosted at
```
http://localhost/docs
```

Afterwards, stop the process and enter `docker compose down`.

## Tests
Install additional dev-dependencies with
```
pip install -r dev-requirements.txt
```
Run unit-tests with
```
pytest -v -s
```

## List of plugins
Part of this implementation is a plugin-system for IE-imports.
It is based on a common interface and can be used to add support for new source-systems.
Currently, the following plugins are pre-defined:
* `demo`: generates test-data in a OAI-PMH-like format (needs to be enabled explicitly)
* `oai_pmh`: import based on the OAI-protocol for metadata harvesting

The expected call signatures for individual plugins are provided via the API (endpoint `GET-/identify`).

## Environment/Configuration
Service-specific environment variables are
* `IE_OUTPUT` [DEFAULT "ie/"] output directory for extracted IEs (relative to `FS_MOUNT_POINT`)
* `SOURCE_SYSTEM_TIMEOUT` [DEFAULT 30] time until a request made to a source system times out in seconds
* `TRANSFER_RETRIES` [DEFAULT 3]: number of retries for failed import
* `TRANSFER_RETRY_INTERVAL` [DEFAULT 360]: interval between retries in seconds
* `USE_TEST_PLUGIN` [DEFAULT 0]: make the test-plugin available
* `IP_BUILDER_HOST` [DEFAULT http://localhost:8083] host address for IP Builder-service
* `IP_BUILDER_JOB_TIMEOUT` [DEFAULT 3600]: time until a job of the ip-builder-service times out in seconds

Additionally this service provides environment options for
* `BaseConfig`,
* `OrchestratedAppConfig`, and
* `FSConfig`

as listed [here](https://github.com/lzv-nrw/dcm-common#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
* Roman Kudinov