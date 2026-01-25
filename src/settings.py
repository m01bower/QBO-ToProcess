"""Settings management for QBO ToProcess."""

import json
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional, Dict, List


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

    def is_configured(self) -> bool:
        """Check if basic settings are present."""
        return bool(self.qbo_app.client_id and self.qbo_app.client_secret)

    def get_enabled_clients(self) -> List[str]:
        """Get list of enabled client names."""
        return [name for name, cfg in self.clients.items() if cfg.enabled]


def get_config_dir() -> Path:
    """Get the config directory path."""
    src_dir = Path(__file__).parent
    project_root = src_dir.parent
    return project_root / "config"


def get_shared_dir() -> Path:
    """Get the shared directory path for cross-project credentials."""
    return Path("D:/Projects/_shared")


def get_settings_path() -> Path:
    """Get the settings.json file path."""
    return get_config_dir() / "settings.json"


def get_clients_path() -> Path:
    """Get the clients.json file path."""
    return get_config_dir() / "clients.json"


def get_service_account_path() -> Path:
    """Get the service_account.json file path."""
    return get_config_dir() / "service_account.json"


def get_qbo_app_path() -> Path:
    """Get the qbo_app.json file path."""
    return get_config_dir() / "qbo_app.json"


def get_qbo_token_path(client_name: str) -> Path:
    """Get the QBO token file path for a client."""
    return get_config_dir() / "qbo_tokens" / f"{client_name.lower()}.json"


def get_google_token_path(client_name: str) -> Path:
    """Get the Google OAuth token file path for a client (shared across projects)."""
    return get_shared_dir() / "google_tokens" / f"{client_name.lower()}.json"


def get_google_credentials_path() -> Path:
    """Get the Google OAuth credentials.json file path (shared across projects)."""
    return get_shared_dir() / "credentials.json"


def load_settings() -> AppSettings:
    """Load settings from config files."""
    settings = AppSettings()

    # Load QBO app settings
    qbo_app_path = get_qbo_app_path()
    if qbo_app_path.exists():
        try:
            with open(qbo_app_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                settings.qbo_app = QBOAppSettings(
                    client_id=data.get("client_id", ""),
                    client_secret=data.get("client_secret", ""),
                    redirect_uri=data.get("redirect_uri", "http://localhost:8080/callback"),
                    environment=data.get("environment", "production"),
                )
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load QBO app settings: {e}")

    # Load client configurations
    clients_path = get_clients_path()
    if clients_path.exists():
        try:
            with open(clients_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for name, cfg in data.items():
                    settings.clients[name] = ClientConfig(
                        name=name,
                        qbo_realm_id=cfg.get("qbo_realm_id", ""),
                        toprocess_sheet_id=cfg.get("toprocess_sheet_id", ""),
                        google_auth_method=cfg.get("google_auth_method", "oauth"),
                        enabled=cfg.get("enabled", True),
                    )
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load client settings: {e}")

    return settings


def save_qbo_app_settings(qbo_app: QBOAppSettings) -> None:
    """Save QBO app settings."""
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)

    with open(get_qbo_app_path(), "w", encoding="utf-8") as f:
        json.dump(asdict(qbo_app), f, indent=2)


def save_clients(clients: Dict[str, ClientConfig]) -> None:
    """Save client configurations."""
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)

    data = {}
    for name, cfg in clients.items():
        data[name] = {
            "qbo_realm_id": cfg.qbo_realm_id,
            "toprocess_sheet_id": cfg.toprocess_sheet_id,
            "google_auth_method": cfg.google_auth_method,
            "enabled": cfg.enabled,
        }

    with open(get_clients_path(), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def save_qbo_token(client_name: str, token_data: dict) -> None:
    """Save QBO OAuth token for a client."""
    token_dir = get_config_dir() / "qbo_tokens"
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
    token_dir = get_shared_dir() / "google_tokens"
    token_dir.mkdir(parents=True, exist_ok=True)

    token_path = get_google_token_path(client_name)
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
    """Ensure the config directory exists and return its path."""
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "qbo_tokens").mkdir(exist_ok=True)
    (config_dir / "google_tokens").mkdir(exist_ok=True)
    return config_dir
