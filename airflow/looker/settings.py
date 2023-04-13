import os
import looker_sdk
from looker_sdk import api_settings
from airflow.hooks.base import BaseHook

looker = BaseHook.get_connection("LOOKER")

class LookerAuthSettings(api_settings.ApiSettings):
    def __init__(self, *args, **kw_args):
        self.my_var = kw_args.pop("my_var")
        super().__init__(*args, **kw_args)

    def read_config(self) -> api_settings.SettingsConfig:
        config = super().read_config()
        # See api_settings.SettingsConfig for required fields
        if self.my_var == "looker":
            config["client_id"] = looker.login
            config["client_secret"] = looker.password
            config["base_url"] = looker.host
            config["verify_ssl"] = 'True'
        return config

#sdk = looker_sdk.init40(config_settings=LookerAuthSettings(my_var="looker"))

