"""Adapter-definition for object validation."""

from dcm_common import services
import dcm_object_validator_sdk


class ObjectValidationAdapter(services.ServiceAdapter):
    """`ServiceAdapter` for the Object Validator service."""
    _SERVICE_NAME = "Object Validator"
    _SDK = dcm_object_validator_sdk

    @property
    def url(self) -> str:
        """Returns service url."""
        return self._url

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ValidationApi(client)

    def _get_api_endpoint(self):
        return self._api_client.validate

    def _build_request_body(self, base_request_body, target):
        if "validation" not in base_request_body:
            base_request_body["validation"] = {}
        if target is not None:
            base_request_body["validation"]["target"] = target
        if "plugins" not in base_request_body["validation"]:
            base_request_body["validation"]["plugins"] = {}
        return base_request_body

    def success(self, info) -> bool:
        return info.report.get("data", {}).get("success", False)

    def valid(self, info) -> bool:
        return info.report.get("data", {}).get("valid", False)
