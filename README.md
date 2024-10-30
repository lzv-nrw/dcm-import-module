# dcm-import-module

This package implements the OpenAPI Specification defined in the [dcm-import-module-api](https://github.com/lzv-nrw/dcm-import-module-api) project of lzv.nrw.

## Run with python
Run the 'DCM Import Module'-app locally with
```
flask run --port=8080
```

## Run with Docker
Use the `compose.yml` to start the `DCM Import Module`-Container as a service:
```
docker compose up
```
(to rebuild use `docker compose build`).

A Swagger UI is hosted at
```
http://localhost/docs
```
while (by-default) the app listens to port `8080`.

Afterwards, stop the process for example with `Ctrl`+`C` and enter `docker compose down`.

The build process requires authentication with `zivgitlab.uni-muenster.de` in order to gain access to the required python dependencies.
The Dockerfiles are configured to use the information from `~/.netrc` for this authentication (a gitlab api-token is required).

## Plugins
### Test plugin
* has to be activated with environment variable `export USE_TEST_PLUGIN=1`
* plugin identifer: `test`
* supports the `args`
  * `randomize`: (optional; default: false) boolean (make variations in metadata)
  * `number`: (optional; default: 1) integer (number of ies to generate)
* example call:
  ```
  curl -X POST "http://localhost:8080/import/external" -H  "accept: application/json" -H  "Content-Type: application/json" -d '{"import": {"plugin": "test", "args": {"randomize": true, "number": 3}, "build": {"configuration": "..."}}}'
  ```

## Test the request to the `IP Builder` service locally in Python
Create a test folder and set the environment variables `FS_MOUNT_POINT` and
`USE_TEST_PLUGIN` with
```
mkdir -p test_folder
export FS_MOUNT_POINT=test_folder
export USE_TEST_PLUGIN=1
```

Run the 'DCM Import Module'-app locally with
```
flask run --port=8080
```

Start the 'DCM IP Builder'-app locally with
```
export FS_MOUNT_POINT=<path to the test_folder inside the dcm-import-module directory>
flask run --port=8083
```

Start the 'DCM Object Validator'-app locally with
```
export FS_MOUNT_POINT=<path to the test_folder inside the dcm-import-module directory>
flask run --port=8082
```

```
curl -X POST "http://localhost:8080/import/external" -H  "accept: application/json" -H  "Content-Type: application/json" -d '{"import": {
    "plugin": "test",
    "args": {"randomize": true, "number": 2},
    "validation": {"modules": ["payload_integrity", "payload_structure"]},
    "build": {"configuration":
        "gASV0AMAAAAAAACMCmRpbGwuX2RpbGyUjAxfY3JlYXRlX3R5cGWUk5QoaACMCl9sb2FkX3R5cGWUk5SMBHR5cGWUhZRSlIwLQnVpbGRDb25maWeUaASMBm9iamVjdJSFlFKUhZR9lCiMCl9fbW9kdWxlX1+UjA9mYWtlX2NvbmZpZ191cmyUjAlDT05WRVJURVKUaAIoaAeMDkNvbnZlcnRlckNsYXNzlGgLhZR9lChoDmgPjAhnZXRfZGljdJRoAIwQX2NyZWF0ZV9mdW5jdGlvbpSTlChoAIwMX2NyZWF0ZV9jb2RllJOUKEMCAAGUSwJLAEsASwJLAUtDQwRkAFMAlE6FlCmMBHNlbGaUjA9zb3VyY2VfbWV0YWRhdGGUhpSMCDxzdHJpbmc+lGgUSwNDAgQBlCkpdJRSlH2UjAhfX25hbWVfX5RoD3NoFE5OdJRSlH2UfZQojA9fX2Fubm90YXRpb25zX1+UfZSMDF9fcXVhbG5hbWVfX5SMF0NvbnZlcnRlckNsYXNzLmdldF9kaWN0lHWGlGKMB19fZG9jX1+UTnV0lFKUjAhidWlsdGluc5SMB3NldGF0dHKUk5RoMGgraBGHlFIwjAZNQVBQRVKUaAIoaAeMC01hcHBlckNsYXNzlGgLhZR9lChoDmgPjAxnZXRfbWV0YWRhdGGUaBYoaBgoQwIAAZRLA0sASwBLA0sBS0NoGmgbKWgcjANrZXmUaB2HlGgfaDlLBmggKSl0lFKUfZRoJGgPc2g5Tk50lFKUfZR9lChoKX2UaCuMGE1hcHBlckNsYXNzLmdldF9tZXRhZGF0YZR1hpRiaC5OdXSUUpRoM2hIaCtoNoeUUjBoLk51dJRSlGg/KGgkaA9oLk6MC19fcGFja2FnZV9flIwAlIwKX19sb2FkZXJfX5ROjAhfX3NwZWNfX5SMEV9mcm96ZW5faW1wb3J0bGlilIwKTW9kdWxlU3BlY5STlCmBlH2UKIwEbmFtZZRoD4wGbG9hZGVylE6MBm9yaWdpbpSMEmZha2VfY29uZmlnX3VybC5weZSMDGxvYWRlcl9zdGF0ZZROjBpzdWJtb2R1bGVfc2VhcmNoX2xvY2F0aW9uc5ROjA1fc2V0X2ZpbGVhdHRylImMB19jYWNoZWSUTnVijAxfX2J1aWx0aW5zX1+UY2J1aWx0aW5zCl9fZGljdF9fCmgRaDBoNmhIaAhoS3UwaCMoaCRoD2guTmhMaE1oTk5oT2hTaF1jYnVpbHRpbnMKX19kaWN0X18KaBFoMGg2aEhoCGhLdTBoM2hLaCtoCIeUUjAu"
      }
    }
}'
```

The json-response should contain a `token`-value
that can be used to get the corresponding report (replace `<token_value>`):
```
curl -X 'GET' \
  'http://localhost:8080/report?token=<token_value>' \
  -H 'accept: application/json'
```
In most cases, it is be more convenient to get this information
via web-browser by simply entering the respective url
```
http://localhost:8080/report?token=<token_value>
```

Finally, delete the test directory with
```
rm -r test_folder
```

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

as listed [here](https://github.com/lzv-nrw/dcm-common/-/tree/dev?ref_type=heads#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
