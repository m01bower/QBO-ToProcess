"""Main entry point for FinancialSysUpdate."""

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
from processors.preflight import run_preflight, run_preflight_from_configs
from processors.verification import VerificationProcessor
from services.notification_service import NotificationService


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
    print("FinancialSysUpdate - Setup Wizard")
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

    Report configs are read from MasterConfig (Reports_{client} tab).

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

    # Load report configs and notification settings from MasterConfig
    from settings import get_master_config
    master = get_master_config()
    master_sheet_id = master.sheet_id
    reports_tab = f"Reports_{client_name}"

    mc_client = master.get_client(client_name)
    # NotificationService created after Sheets auth so we can pass credentials
    notifier = None  # initialized after sheets.authenticate()

    mc_reports = master.get_qbo_reports(client_name)
    mc_year = master.get_qbo_report_year(client_name)

    if not mc_reports:
        logger.error(f"No report configs found in MasterConfig tab '{reports_tab}'")
        return 1
    if mc_year is None:
        logger.error(f"No year found in MasterConfig tab '{reports_tab}' row 2")
        return 1

    # Convert MasterConfig reports to processor dict format
    year_val, configs = SheetsService.configs_from_master(mc_reports, mc_year, master_sheet_id)

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Client: {client_name}")
    logger.info(f"Config source: MasterConfig/{reports_tab} ({len(configs)} reports, year {year_val})")
    logger.info(f"{'=' * 60}")

    # Initialize Google Sheets service (always use BosOpt credentials for Sheets)
    sheets = SheetsService(
        auth_method=client_config.google_auth_method,
        client_name="BosOpt",
    )
    if not sheets.authenticate():
        logger.error(f"Failed to authenticate with Google Sheets for {client_name}")
        return 1

    # Now that Sheets is authenticated, create NotificationService with shared
    # credentials so Gmail reuses the same access token (includes gmail.send scope)
    notifier = NotificationService(
        client_name, mc_client.notifications,
        google_credentials=sheets._credentials,
    )

    # Initialize QBO service
    qbo = QBOService(settings.qbo_app, client_name)

    if not qbo.is_authenticated:
        logger.error(f"{client_name} not authorized. Run: --auth {client_name}")
        return 1

    # ── Pre-flight checks ──
    preflight_result = run_preflight_from_configs(qbo, sheets, configs)

    if not preflight_result.all_passed:
        logger.error("\nPre-flight checks FAILED:")
        failure_lines = []
        for failure in preflight_result.failures:
            logger.error(f"  \u2717 {failure['name']}: {failure['detail']}")
            failure_lines.append(f"  {failure['name']}: {failure['detail']}")
        logger.error("\nAborting. Fix the issues above and try again.")
        notifier.send_alert(
            f"Pre-flight FAILED — aborting run\n" + "\n".join(failure_lines)
        )
        return 1

    # ── Two-phase processing ──
    processor = ReportProcessor(qbo, sheets)
    results = processor.process_all_reports(
        master_sheet_id,
        configs=configs,
        year=year_val,
        reports_tab=reports_tab,
    )

    # Send per-error alerts
    report_results = {k: v for k, v in results.items() if not k.startswith("_row_change_")}
    for report, result in report_results.items():
        if result.get("status") == "error":
            notifier.send_alert(f"{report}: {result.get('error', 'Unknown error')}")

    # ── Post-write verification ──
    # In test mode, swap verification sheet IDs to test copies so we can
    # run full verification (including writes) without touching production.
    verify_sheets_config = mc_client.sheets
    if sheet_override is not None:
        from dataclasses import replace
        overrides = {}
        if settings.test_toprocess_sheet_id:
            overrides["toprocess_sheet_id"] = settings.test_toprocess_sheet_id
        if settings.test_financial_dashboard_sheet_id:
            overrides["financial_dashboard_sheet_id"] = settings.test_financial_dashboard_sheet_id
        if settings.test_ar_sheet_id:
            overrides["ar_sheet_id"] = settings.test_ar_sheet_id
        if settings.test_total_cash_sheet_id:
            overrides["total_cash_sheet_id"] = settings.test_total_cash_sheet_id
        if overrides:
            verify_sheets_config = replace(verify_sheets_config, **overrides)
            logger.info("Test mode — verification using test sheet IDs")

    verifier = VerificationProcessor(sheets, verify_sheets_config, year_val)
    verification = verifier.run(results)

    # Print report summary to console
    print(f"\n{'=' * 60}")
    print(f"SUMMARY — {client_name}")
    print(f"{'=' * 60}")

    row_changes = {k: v for k, v in results.items() if k.startswith("_row_change_")}

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

    if row_changes:
        print(f"\n{'!' * 60}")
        print("ROW CHANGES DETECTED — verify dependent tabs:")
        print(f"{'!' * 60}")
        for change_key, info in row_changes.items():
            tab = info.get("tab", "")
            added = info.get("rows_added", 0)
            print(f"  \u26a0 {tab}: {added} row(s) added — check formulas in other tabs")

    # Print verification summary
    for line in verification.summary_lines():
        print(line)

    # Send verification failures as alerts
    for check in verification.checks:
        if not check.passed:
            notifier.send_alert(f"Verification FAIL: {check.name} — {check.detail}")

    # Send notifications (include verification in summary)
    verification_text = "\n".join(verification.summary_lines())
    notifier.send_summary(results, year_val, verification_text)

    has_errors = error_count > 0 or not verification.all_passed
    return 0 if not has_errors else 1


def process_all_clients(settings: AppSettings) -> int:
    """
    Process all enabled ToProcess clients in sequence.

    Each client runs independently — a failure in one does not stop the others.

    Returns:
        Exit code (0 if all succeeded, 1 if any failed)
    """
    logger = get_logger()

    from settings import get_master_config
    master = get_master_config()
    active_clients = master.get_active_clients("QBO", tool_feature="toprocess_active")

    if not active_clients:
        logger.error("No active ToProcess clients found in MasterConfig")
        return 1

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Processing {len(active_clients)} clients: {', '.join(active_clients)}")
    logger.info(f"{'=' * 60}")

    client_results = {}
    for client_name in active_clients:
        logger.info(f"\n>>> Starting {client_name}")
        try:
            exit_code = process_client(client_name, settings)
            client_results[client_name] = exit_code
        except Exception as e:
            logger.error(f"Unhandled error processing {client_name}: {e}")
            client_results[client_name] = 1

    # Print overall summary
    print(f"\n{'=' * 60}")
    print(f"ALL CLIENTS SUMMARY")
    print(f"{'=' * 60}\n")

    all_ok = True
    for client_name, code in client_results.items():
        status = "\u2713" if code == 0 else "\u2717"
        print(f"  {status} {client_name}")
        if code != 0:
            all_ok = False

    passed = sum(1 for c in client_results.values() if c == 0)
    failed = sum(1 for c in client_results.values() if c != 0)
    print(f"\n{passed} passed, {failed} failed")

    return 0 if all_ok else 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="FinancialSysUpdate - Export QuickBooks reports to Google Sheets"
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
        help="Client to process (or 'all' for all enabled clients)",
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

    # Determine client(s)
    if not args.client:
        print("Error: --client is required. Specify a client name or 'all'.")
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

    if args.client.lower() == "all":
        if sheet_override:
            print("Error: --sheet/--test cannot be used with --client all")
            sys.exit(1)
        exit_code = process_all_clients(settings)
    else:
        exit_code = process_client(args.client, settings, sheet_override=sheet_override)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
