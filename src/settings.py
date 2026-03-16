"""Settings management for QBO ToProcess."""

import json
import sys
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional, Dict, List

import keyring

# Add _shared_config to sys.path so we can import config_reader
_SHARED_CONFIG_DIR = Path(__file__).parent.parent.parent / "_shared_config"
if str(_SHARED_CONFIG_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_CONFIG_DIR))


@dataclass
class QBOAppSettings:
    """QuickBooks OAuth app settings."""
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = "http://localhost:8080/callback"
    environment: str = "production"  # "sandbox" or "production"


@dataclass
class ClientConfig:
    """Configuration for a single client."""
    name: str
    qbo_realm_id: str = ""  # QuickBooks company ID
    toprocess_sheet_id: str = ""  # Google Sheet ID containing ToProcess tab
    google_auth_method: str = "oauth"  # "oauth" or "service_account"
    enabled: bool = True


@dataclass
class AppSettings:
    """Application settings."""
    qbo_app: QBOAppSettings = field(default_factory=QBOAppSettings)
    clients: Dict[str, ClientConfig] = field(default_factory=dict)
    use_service_account: bool = True
    test_toprocess_sheet_id: str = ""

    def is_configured(self) -> bool:
        """Check if basic settings are present."""
        return bool(self.qbo_app.client_id and self.qbo_app.client_secret)

    def get_enabled_clients(self) -> List[str]:
        """Get list of enabled client names."""
        return [name for name, cfg in self.clients.items() if cfg.enabled]


_SHARED_APP_DIR = _SHARED_CONFIG_DIR / "apps" / "QBO_ToProcess"


def get_config_dir() -> Path:
    """Get the app config directory in _shared_config."""
    return _SHARED_APP_DIR


def get_shared_dir() -> Path:
    """Get the shared directory path for cross-project credentials."""
    return _SHARED_CONFIG_DIR


def get_settings_path() -> Path:
    """Get the settings.json file path."""
    return _SHARED_APP_DIR / "settings.json"


def get_service_account_path() -> Path:
    """Get the service_account.json file path."""
    return _SHARED_APP_DIR / "service_account.json"


def get_qbo_app_path() -> Path:
    """Get the qbo_app.json file path."""
    return _SHARED_APP_DIR / "qbo_app.json"


def get_qbo_token_path(client_name: str) -> Path:
    """Get the QBO token file path for a client."""
    return _SHARED_APP_DIR / "qbo_tokens" / f"{client_name.lower()}.json"


def get_google_token_path(client_name: str) -> Path:
    """Get the Google OAuth token file path for a client (shared across projects)."""
    return get_shared_dir() / "clients" / client_name / "token.json"


def get_google_credentials_path(client_name: str = None) -> Path:
    """
    Get the Google OAuth credentials.json file path (shared across projects).

    Checks for client-specific credentials first, falls back to shared default.
    """
    if client_name:
        client_creds = get_shared_dir() / "clients" / client_name / "credentials.json"
        if client_creds.exists():
            return client_creds
    return get_shared_dir() / "credentials.json"


def _load_master_config():
    """
    Load MasterConfig from the shared config reader.

    Returns:
        MasterConfig instance.

    Raises:
        RuntimeError: If MasterConfig cannot be loaded.
    """
    try:
        from config_reader import MasterConfig
        return MasterConfig()
    except Exception as e:
        raise RuntimeError(
            f"Cannot load master config — check network and "
            f"_shared_config/master_config.json: {e}"
        ) from e


# Module-level cache so we only read the master sheet once per run
_master_config_cache = None


def get_master_config():
    """Get the cached MasterConfig instance (loads on first call)."""
    global _master_config_cache
    if _master_config_cache is None:
        _master_config_cache = _load_master_config()
    return _master_config_cache


def load_settings() -> AppSettings:
    """
    Load settings from MasterConfig (client data) and local files (secrets only).

    QBO app credentials (client_id, client_secret) come from the OS keyring
    (service "BosOpt"). Remaining QBO app settings (redirect_uri, environment)
    come from local qbo_app.json. Client config (realm_id, sheet IDs, auth
    method, enabled) comes exclusively from MasterConfig.

    Raises:
        RuntimeError: If MasterConfig cannot be loaded or client data cannot
            be read from it.
    """
    settings = AppSettings()

    # Load QBO app settings:
    #   client_id / client_secret  -> OS keyring (service "BosOpt")
    #   redirect_uri / environment -> local qbo_app.json
    redirect_uri = "http://localhost:8080/callback"
    environment = "production"
    test_toprocess_sheet_id = ""
    qbo_app_path = get_qbo_app_path()
    if qbo_app_path.exists():
        try:
            with open(qbo_app_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                redirect_uri = data.get("redirect_uri", redirect_uri)
                environment = data.get("environment", environment)
                test_toprocess_sheet_id = data.get("test_toprocess_sheet_id", "")
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load QBO app settings from file: {e}")

    # Pick sandbox or production keys from keyring based on environment
    is_sandbox = environment.lower() == "sandbox"
    client_id = keyring.get_password(
        "BosOpt", "QBO-Sandbox-ClientID" if is_sandbox else "QBO-ClientID"
    ) or ""
    client_secret = keyring.get_password(
        "BosOpt", "QBO-Sandbox-ClientSecret" if is_sandbox else "QBO-ClientSecret"
    ) or ""
    if not client_id or not client_secret:
        env_label = "Sandbox" if is_sandbox else "Production"
        print(f"Warning: QBO {env_label} client_id/client_secret not found in keyring")

    settings.qbo_app = QBOAppSettings(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        environment=environment,
    )
    settings.test_toprocess_sheet_id = test_toprocess_sheet_id

    # Load client config from MasterConfig (the only source of truth)
    master = get_master_config()

    # Get clients that have QBO ToProcess active
    active_keys = master.get_active_clients("QBO", tool_feature="toprocess_active")

    for client_key in master.list_clients():
        mc = master.get_client(client_key)
        # A client is "enabled" for this app if toprocess_active is set
        is_enabled = client_key in active_keys

        # Environment override: prefer master config, fall back to qbo_app.json
        if mc.qbo.environment and mc.qbo.environment != "sandbox":
            settings.qbo_app.environment = mc.qbo.environment

        settings.clients[client_key] = ClientConfig(
            name=client_key,
            qbo_realm_id=mc.qbo.realm_id,
            toprocess_sheet_id=mc.sheets.toprocess_sheet_id,
            google_auth_method=mc.qbo.google_auth_method or "oauth",
            enabled=is_enabled,
        )

    print(f"Loaded {len(settings.clients)} clients from MasterConfig "
          f"({len(active_keys)} ToProcess-active)")

    return settings


def save_qbo_app_settings(qbo_app: QBOAppSettings) -> None:
    """Save QBO app settings."""
    _SHARED_APP_DIR.mkdir(parents=True, exist_ok=True)

    with open(get_qbo_app_path(), "w", encoding="utf-8") as f:
        json.dump(asdict(qbo_app), f, indent=2)


def save_qbo_token(client_name: str, token_data: dict) -> None:
    """Save QBO OAuth token for a client."""
    token_dir = _SHARED_APP_DIR / "qbo_tokens"
    token_dir.mkdir(parents=True, exist_ok=True)

    token_path = get_qbo_token_path(client_name)
    with open(token_path, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)


def load_qbo_token(client_name: str) -> Optional[dict]:
    """Load QBO OAuth token for a client."""
    token_path = get_qbo_token_path(client_name)
    if not token_path.exists():
        return None

    try:
        with open(token_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def save_google_token(client_name: str, token_data: dict) -> None:
    """Save Google OAuth token for a client (shared across projects)."""
    token_path = get_google_token_path(client_name)
    token_path.parent.mkdir(parents=True, exist_ok=True)

    with open(token_path, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)


def load_google_token(client_name: str) -> Optional[dict]:
    """Load Google OAuth token for a client."""
    token_path = get_google_token_path(client_name)
    if not token_path.exists():
        return None

    try:
        with open(token_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def ensure_config_dir() -> Path:
    """Ensure the app config directory exists and return its path."""
    _SHARED_APP_DIR.mkdir(parents=True, exist_ok=True)
    (_SHARED_APP_DIR / "qbo_tokens").mkdir(exist_ok=True)
    return _SHARED_APP_DIR
