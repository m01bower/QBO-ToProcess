"""Main entry point for QBO ToProcess."""

import sys
import argparse
from typing import Dict, List, Optional

from config import CLIENTS
from settings import (
    AppSettings,
    QBOAppSettings,
    ClientConfig,
    load_settings,
    save_qbo_app_settings,
    save_clients,
    ensure_config_dir,
    get_service_account_path,
)
from logger_setup import setup_logger, get_logger
from services.qbo_service import QBOService
from services.sheets_service import SheetsService
from processors.report_processor import ReportProcessor
from gui.client_selector import select_clients


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

    # Step 3: Client Configuration
    print("\n" + "-" * 40)
    print("Step 3: Client Configuration")
    print("-" * 40)
    print(f"\nConfiguring {len(CLIENTS)} clients: {', '.join(CLIENTS)}\n")

    clients: Dict[str, ClientConfig] = {}

    for client_name in CLIENTS:
        print(f"\n--- {client_name} ---")
        sheet_id = get_input(f"  ToProcess Google Sheet ID (or skip): ").strip()

        if not sheet_id:
            print(f"  Skipping {client_name}")
            clients[client_name] = ClientConfig(
                name=client_name,
                enabled=False,
            )
            continue

        # Ask for Google auth method
        auth_method = get_input(f"  Google auth method (oauth/service_account) [oauth]: ").strip().lower()
        if auth_method not in ["oauth", "service_account", ""]:
            print("  Invalid auth method, defaulting to oauth")
            auth_method = "oauth"
        auth_method = auth_method or "oauth"

        clients[client_name] = ClientConfig(
            name=client_name,
            toprocess_sheet_id=sheet_id,
            google_auth_method=auth_method,
            enabled=True,
        )
        print(f"  {client_name} configured ({auth_method})!")

    save_clients(clients)

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
            print(f"  ✓ {client_name} authorized!")
            # Update realm_id in client config
            if qbo._realm_id:
                config.qbo_realm_id = qbo._realm_id
        else:
            print(f"  ✗ {client_name} authorization failed")

    save_clients(clients)

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
        print(f"\n✓ {client_name} authorized successfully!")

        # Update client config with realm_id
        if qbo._realm_id:
            settings.clients[client_name].qbo_realm_id = qbo._realm_id
            save_clients(settings.clients)

        return True
    else:
        print(f"\n✗ {client_name} authorization failed")
        return False


def process_clients(client_names: List[str]) -> int:
    """
    Process reports for specified clients.

    Args:
        client_names: List of client names to process

    Returns:
        Exit code (0 for success)
    """
    logger = get_logger()
    settings = load_settings()

    if not settings.is_configured():
        logger.error("App not configured. Run --setup first.")
        return 1

    all_results = {}
    error_count = 0

    for client_name in client_names:
        if client_name not in settings.clients:
            logger.error(f"Unknown client: {client_name}")
            continue

        client_config = settings.clients[client_name]

        if not client_config.enabled:
            logger.warning(f"Client {client_name} is disabled, skipping")
            continue

        if not client_config.toprocess_sheet_id:
            logger.error(f"No ToProcess sheet ID for {client_name}")
            continue

        logger.info(f"\n{'=' * 40}")
        logger.info(f"Processing: {client_name}")
        logger.info(f"{'=' * 40}")

        # Initialize Sheets service with client's auth method
        sheets = SheetsService(
            auth_method=client_config.google_auth_method,
            client_name=client_name,
        )
        if not sheets.authenticate():
            logger.error(f"  Failed to authenticate with Google Sheets for {client_name}")
            error_count += 1
            continue

        # Initialize QBO service for this client
        qbo = QBOService(settings.qbo_app, client_name)

        if not qbo.is_authenticated:
            logger.error(f"  {client_name} not authorized. Run: --auth {client_name}")
            error_count += 1
            continue

        if not qbo.test_connection():
            logger.error(f"  Failed to connect to QBO for {client_name}")
            error_count += 1
            continue

        # Verify sheet access
        if not sheets.verify_sheet_access(client_config.toprocess_sheet_id):
            logger.error(f"  Cannot access ToProcess sheet for {client_name}")
            error_count += 1
            continue

        # Process reports
        processor = ReportProcessor(qbo, sheets)
        results = processor.process_all_reports(client_config.toprocess_sheet_id)

        all_results[client_name] = results

        # Count errors for this client
        client_errors = sum(1 for r in results.values() if r.get("status") == "error")
        if client_errors > 0:
            error_count += client_errors

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for client_name, results in all_results.items():
        success = sum(1 for r in results.values() if r.get("status") == "success")
        errors = sum(1 for r in results.values() if r.get("status") == "error")
        print(f"\n{client_name}: {success} succeeded, {errors} failed")

        for report, result in results.items():
            status = result.get("status", "unknown")
            rows = result.get("rows", 0)
            error = result.get("error", "")

            if status == "success":
                print(f"  ✓ {report}: {rows} rows")
            else:
                print(f"  ✗ {report}: {error}")

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
        action="append",
        help="Process specific client(s) - can be used multiple times",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all enabled clients without GUI",
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

    # Determine which clients to process
    if args.client:
        # Specific clients from command line
        clients_to_process = args.client
    elif args.all:
        # All enabled clients
        clients_to_process = settings.get_enabled_clients()
        if not clients_to_process:
            print("No enabled clients found. Run --setup to configure.")
            sys.exit(1)
    else:
        # Show GUI for selection
        enabled_clients = settings.get_enabled_clients()
        if not enabled_clients:
            print("No enabled clients found. Run --setup to configure.")
            sys.exit(1)

        client_status = {
            name: cfg.enabled
            for name, cfg in settings.clients.items()
        }

        selected = select_clients(enabled_clients, client_status)

        if not selected:
            print("Operation cancelled")
            sys.exit(0)

        clients_to_process = selected

    # Process the selected clients
    exit_code = process_clients(clients_to_process)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
