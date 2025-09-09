"""Adapter-definition for IP-building."""

from dcm_common import services
import dcm_ip_builder_sdk


class BuildAdapter(services.ServiceAdapter):
    """`ServiceAdapter` for the IP Builder service."""

    _SERVICE_NAME = "IP Builder"
    _SDK = dcm_ip_builder_sdk

    @property
    def url(self) -> str:
        """Returns service url."""
        return self._url

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.BuildApi(client)

    def _get_api_endpoint(self):
        return self._api_client.build

    def _get_abort_endpoint(self):
        return self._api_client.abort_build

    def _build_request_body(self, base_request_body, target):
        if target is not None:
            if "build" not in base_request_body:
                base_request_body["build"] = {}
            base_request_body["build"]["target"] = target
        return base_request_body

    def success(self, info) -> bool:
        return info.report.get("data", {}).get("success", False)

    def valid(self, info) -> bool:
        return info.report.get("data", {}).get("valid", False)
