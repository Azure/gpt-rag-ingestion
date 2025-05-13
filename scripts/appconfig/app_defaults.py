#!/usr/bin/env python3
import os
import sys
import json
import logging
import re

from dotenv import load_dotenv
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.appconfiguration import AzureAppConfigurationClient, ConfigurationSetting

# Load environment variables from .env (if present)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for logger_name in (
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.identity",
    "azure.appconfiguration"
):
    logging.getLogger(logger_name).setLevel(logging.WARNING)


def load_defaults(path):
    """
    Read the app_defaults.env file and return a dict of {KEY: raw_value}.
    Ignores blank lines and lines starting with '#'.
    """
    defaults = {}
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, val = line.split('=', 1)
            val = val.strip().strip('"').strip("'")
            defaults[key] = val
    return defaults


def resolve_placeholders(raw_val, infra_cfg):
    """
    Replace placeholders in raw_val of the forms:
      {A}, {A.B}, or {A.B.C}
    Lookup rules:
      - {A}: get infra_cfg['A']
      - {A.B}: if infra_cfg['A'] is dict, return dict['B']; 
                if list, find element with internal_name=='B' and return its JSON
      - {A.B.C}: infra_cfg['A'] is list; find element with internal_name=='B' and return element['C']
    """
    def _replace(match):
        placeholder = match.group(1)
        parts = placeholder.split('.')
        top = parts[0]

        if top not in infra_cfg:
            logging.warning(f"Missing infra setting for placeholder {{{placeholder}}}")
            return match.group(0)

        # Single-level placeholder {A}
        if len(parts) == 1:
            return str(infra_cfg[top])

        # Two-level placeholder {A.B}
        if len(parts) == 2:
            cfg = infra_cfg[top]
            key2 = parts[1]
            if isinstance(cfg, dict):
                return str(cfg.get(key2, match.group(0)))
            if isinstance(cfg, list):
                for elem in cfg:
                    if elem.get('internal_name') == key2:
                        return json.dumps(elem)
            return match.group(0)

        # Three-level placeholder {A.B.C}
        if len(parts) == 3:
            list_name, internal, attr = parts
            cfg_list = infra_cfg.get(list_name, [])
            if isinstance(cfg_list, list):
                for elem in cfg_list:
                    if elem.get('internal_name') == internal:
                        return str(elem.get(attr, match.group(0)))
            logging.warning(f"Could not resolve nested placeholder {{{placeholder}}}")
            return match.group(0)

        # Unsupported format
        logging.warning(f"Unsupported placeholder format {{{placeholder}}}")
        return match.group(0)

    return re.sub(r'\{([^}]+)\}', _replace, raw_val)


def main():
    # 1) Ensure AZURE_APP_CONFIG_ENDPOINT is provided
    endpoint = os.getenv("AZURE_APP_CONFIG_ENDPOINT")
    if not endpoint:
        logging.error("AZURE_APP_CONFIG_ENDPOINT not set")
        sys.exit(1)

    # Authenticate using Managed Identity or Azure CLI  
    cred = ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())
    client = AzureAppConfigurationClient(endpoint, cred)

    # 2) Load infra settings (label="infra")
    infra_cfg = {}
    for setting in client.list_configuration_settings(label_filter="infra"):
        try:
            infra_cfg[setting.key] = json.loads(setting.value)
        except json.JSONDecodeError:
            infra_cfg[setting.key] = setting.value

    # 3) Collect existing dataingest keys
    existing_keys = {
        s.key
        for s in client.list_configuration_settings(label_filter="dataingest")
    }

    # 4) Load defaults from relative path
    defaults = load_defaults("scripts/appconfig/app_defaults.env")

    # 5) Iterate defaults: if missing, resolve placeholders and create
    for key, raw_val in defaults.items():
        if key in existing_keys:
            logging.info(f"→ Skipping existing key: {key}")
            continue

        # Perform placeholder substitution
        final_val = resolve_placeholders(raw_val, infra_cfg)

        # Wrap into ConfigurationSetting and push
        setting = ConfigurationSetting(
            key=key,
            value=final_val,
            label="dataingest"
        )
        client.set_configuration_setting(setting)
        logging.info(f"✔ Created key: {key} (label=dataingest)")

    logging.info("✅ All default settings validated and applied.")


if __name__ == "__main__":
    main()
