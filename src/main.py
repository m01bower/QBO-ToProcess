"""Main entry point for QBO ToProcess."""

import sys
import argparse
from typing import Dict, List, Optional

from settings import (
    AppSettings,
    QBOAppSettings,
    ClientConfig,
    load_settings,
    save_qbo_app_settings,
    ensure_config_dir,
    get_service_account_path,
)
from logger_setup import setup_logger, get_logger
from services.qbo_service import QBOService
from services.sheets_service import SheetsService
from processors.report_processor import ReportProcessor
from processors.preflight import run_preflight


def run_setup() -> bool:
    """
    Run the setup wizard to configure the application.

    Returns:
        True if setup completed successfully
    """
    logger = get_logger()

    def get_input(prompt: str) -> str:
        """Get input with Ctrl+C handling."""
        try:
            return input(prompt)
        except (KeyboardInterrupt, EOFError):
            print("\n\nSetup cancelled.")
            sys.exit(0)

    print("\n" + "=" * 60)
    print("QBO ToProcess - Setup Wizard")
    print("(Press Ctrl+C to cancel)")
    print("=" * 60 + "\n")

    config_dir = ensure_config_dir()
    print(f"Config directory: {config_dir}\n")

    # Step 1: QuickBooks App Credentials
    print("-" * 40)
    print("Step 1: QuickBooks Developer App")
    print("-" * 40)
    print("\nTo get QuickBooks API credentials:")
    print("  1. Go to https://developer.intuit.com/app/developer/dashboard")
    print("  2. Create a new app (or use existing)")
    print("  3. Go to app settings -> Keys & credentials")
    print("  4. Copy Client ID and Client Secret")
    print("  5. Add redirect URI: http://localhost:8080/callback\n")

    client_id = get_input("Enter QBO Client ID: ").strip()
    if not client_id:
        print("Error: Client ID is required")
        return False

    client_secret = get_input("Enter QBO Client Secret: ").strip()
    if not client_secret:
        print("Error: Client Secret is required")
        return False

    env = get_input("Environment (production/sandbox) [production]: ").strip().lower()
    if env not in ["production", "sandbox", ""]:
        print("Error: Invalid environment")
        return False
    env = env or "production"

    qbo_app = QBOAppSettings(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="http://localhost:8080/callback",
        environment=env,
    )
    save_qbo_app_settings(qbo_app)
    print("QBO app settings saved!")

    # Step 2: Google Authentication Setup
    print("\n" + "-" * 40)
    print("Step 2: Google Sheets Authentication")
    print("-" * 40)
    print("\nYou can use OAuth (browser login) or Service Account per client.")
    print("\nFor OAuth (recommended for most clients):")
    print("  1. Go to https://console.cloud.google.com")
    print("  2. Create or select a project")
    print("  3. Enable the Google Sheets API")
    print("  4. Go to 'APIs & Services' -> 'Credentials'")
    print("  5. Create OAuth 2.0 Client ID (Desktop app)")
    print("  6. Download JSON and save as: config/google_credentials.json")
    print("\nFor Service Account (for clients without your Google access):")
    print("  1. Create a Service Account in the same project")
    print("  2. Download JSON key as: config/service_account.json")
    print("  3. Share the spreadsheet with the service account email\n")

    # Step 3: Client Configuration (from MasterConfig)
    print("\n" + "-" * 40)
    print("Step 3: Client Configuration")
    print("-" * 40)

    # Get client list from MasterConfig (required)
    from settings import get_master_config
    try:
        master = get_master_config()
    except RuntimeError as e:
        print(f"\nError: {e}")
        return False
    setup_clients = master.list_clients()
    print(f"\nLoaded {len(setup_clients)} clients from MasterConfig")
    print("(Client config — sheet IDs, auth methods, enabled flags — is managed in MasterConfig)\n")

    # Build client configs from MasterConfig for the auth step
    active_keys = master.get_active_clients("QBO", tool_feature="toprocess_active")
    clients: Dict[str, ClientConfig] = {}
    for client_key in setup_clients:
        mc = master.get_client(client_key)
        clients[client_key] = ClientConfig(
            name=client_key,
            qbo_realm_id=mc.qbo.realm_id,
            toprocess_sheet_id=mc.sheets.toprocess_sheet_id,
            google_auth_method=mc.qbo.google_auth_method or "oauth",
            enabled=client_key in active_keys,
        )

    for name, cfg in clients.items():
        status = "enabled" if cfg.enabled else "disabled"
        print(f"  {name}: {status}")

    # Step 4: QBO OAuth per client
    print("\n" + "-" * 40)
    print("Step 4: QuickBooks Authorization")
    print("-" * 40)
    print("\nNow authorize each client's QuickBooks account.")
    print("A browser window will open for each client.\n")

    for client_name, config in clients.items():
        if not config.enabled:
            continue

        authorize = get_input(f"Authorize {client_name} now? (y/n): ").strip().lower()
        if authorize != "y":
            print(f"  Skipping {client_name} - you can authorize later with --auth {client_name}")
            continue

        qbo = QBOService(qbo_app, client_name)
        if qbo.authenticate_interactive():
            print(f"  \u2713 {client_name} authorized!")
            # Update realm_id in client config
            if qbo._realm_id:
                config.qbo_realm_id = qbo._realm_id
        else:
            print(f"  \u2717 {client_name} authorization failed")

    print("\n" + "=" * 60)
    print("Setup Complete!")
    print("=" * 60)
    print("\nRun the application with: python src/main.py")
    print("Or authorize a specific client: python src/main.py --auth ClientName")

    return True


def authorize_client(client_name: str) -> bool:
    """
    Authorize a specific client's QuickBooks account.

    Args:
        client_name: Name of the client to authorize

    Returns:
        True if authorization successful
    """
    logger = get_logger()
    settings = load_settings()

    if not settings.is_configured():
        print("Error: App not configured. Run --setup first.")
        return False

    if client_name not in settings.clients:
        print(f"Error: Unknown client '{client_name}'")
        print(f"Available clients: {', '.join(settings.clients.keys())}")
        return False

    print(f"\nAuthorizing {client_name}...")
    print("A browser window will open. Please log in and authorize the app.\n")

    qbo = QBOService(settings.qbo_app, client_name)
    if qbo.authenticate_interactive():
        print(f"\n\u2713 {client_name} authorized successfully!")

        return True
    else:
        print(f"\n\u2717 {client_name} authorization failed")
        return False


def process_client(client_name: str, settings: AppSettings,
                   sheet_override: Optional[str] = None) -> int:
    """
    Process reports for a single client using preflight + two-phase approach.

    Args:
        client_name: Client name to process
        settings: Loaded app settings
        sheet_override: Optional override for the ToProcess sheet ID (for testing)

    Returns:
        Exit code (0 for success)
    """
    logger = get_logger()

    if client_name not in settings.clients:
        logger.error(f"Unknown client: {client_name}")
        print(f"Available clients: {', '.join(settings.clients.keys())}")
        return 1

    client_config = settings.clients[client_name]

    if not client_config.enabled and not sheet_override:
        logger.warning(f"Client {client_name} is disabled in MasterConfig")
        return 1

    toprocess_sheet_id = sheet_override or client_config.toprocess_sheet_id
    if not toprocess_sheet_id:
        logger.error(f"No ToProcess sheet ID for {client_name}")
        return 1

    if sheet_override:
        logger.info(f"Using sheet override: {sheet_override}")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Client: {client_name}")
    logger.info(f"ToProcess Sheet: {toprocess_sheet_id}")
    logger.info(f"{'=' * 60}")

    # Initialize Google Sheets service (always use BosOpt credentials for Sheets)
    sheets = SheetsService(
        auth_method=client_config.google_auth_method,
        client_name="BosOpt",
    )
    if not sheets.authenticate():
        logger.error(f"Failed to authenticate with Google Sheets for {client_name}")
        return 1

    # Initialize QBO service
    qbo = QBOService(settings.qbo_app, client_name)

    if not qbo.is_authenticated:
        logger.error(f"{client_name} not authorized. Run: --auth {client_name}")
        return 1

    # ── Pre-flight checks ──
    preflight_result, configs = run_preflight(qbo, sheets, toprocess_sheet_id)

    if not preflight_result.all_passed:
        logger.error("\nPre-flight checks FAILED:")
        for failure in preflight_result.failures:
            logger.error(f"  \u2717 {failure['name']}: {failure['detail']}")
        logger.error("\nAborting. Fix the issues above and try again.")
        return 1

    # Retrieve year from configs (preflight already read it)
    # Re-read just the year since preflight validated it exists
    year_val, _ = sheets.read_toprocess_config(toprocess_sheet_id)
    if year_val is None:
        logger.error("Could not determine report year")
        return 1

    # ── Two-phase processing ──
    processor = ReportProcessor(qbo, sheets)
    results = processor.process_all_reports(
        toprocess_sheet_id,
        configs=configs,
        year=year_val,
    )

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"SUMMARY — {client_name}")
    print(f"{'=' * 60}")

    # Separate row-change flags from report results
    row_changes = {k: v for k, v in results.items() if k.startswith("_row_change_")}
    report_results = {k: v for k, v in results.items() if not k.startswith("_row_change_")}

    success_count = sum(1 for r in report_results.values() if r.get("status") == "success")
    error_count = sum(1 for r in report_results.values() if r.get("status") == "error")

    print(f"\n{success_count} succeeded, {error_count} failed\n")

    for report, result in report_results.items():
        status = result.get("status", "unknown")
        rows = result.get("rows", 0)
        error = result.get("error", "")

        if status == "success":
            print(f"  \u2713 {report}: {rows} rows")
        else:
            print(f"  \u2717 {report}: {error}")

    # Alert on row changes that may require manual verification
    if row_changes:
        print(f"\n{'!' * 60}")
        print("ROW CHANGES DETECTED — verify dependent tabs:")
        print(f"{'!' * 60}")
        for change_key, info in row_changes.items():
            tab = info.get("tab", "")
            added = info.get("rows_added", 0)
            print(f"  \u26a0 {tab}: {added} row(s) inserted — check formulas in other tabs")

    return 0 if error_count == 0 else 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="QBO ToProcess - Export QuickBooks reports to Google Sheets"
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Run the setup wizard",
    )
    parser.add_argument(
        "--auth",
        metavar="CLIENT",
        help="Authorize a specific client's QuickBooks account",
    )
    parser.add_argument(
        "--client",
        metavar="CLIENT",
        required=False,
        help="Client to process",
    )
    parser.add_argument(
        "--sheet",
        metavar="SHEET_ID",
        required=False,
        help="Override the ToProcess sheet ID (for testing with a sandbox sheet)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use the test_toprocess_sheet_id from qbo_app.json config",
    )

    args = parser.parse_args()

    # Set up logging
    logger = setup_logger()

    # Run setup if requested
    if args.setup:
        success = run_setup()
        sys.exit(0 if success else 1)

    # Authorize specific client if requested
    if args.auth:
        success = authorize_client(args.auth)
        sys.exit(0 if success else 1)

    # Load settings
    settings = load_settings()

    if not settings.is_configured():
        print("Application not configured. Run with --setup first:")
        print("  python src/main.py --setup")
        sys.exit(1)

    # Determine client
    if not args.client:
        print("Error: --client is required. Specify which client to process.")
        print(f"Available clients: {', '.join(settings.get_enabled_clients())}")
        sys.exit(1)

    # Determine sheet override
    sheet_override = args.sheet
    if args.test:
        if not settings.test_toprocess_sheet_id:
            print("Error: --test flag used but no test_toprocess_sheet_id in qbo_app.json")
            sys.exit(1)
        sheet_override = settings.test_toprocess_sheet_id
        print(f"TEST MODE: Using test sheet {sheet_override}")

    exit_code = process_client(args.client, settings, sheet_override=sheet_override)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
