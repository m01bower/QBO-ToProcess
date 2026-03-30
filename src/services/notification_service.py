"""Notification service for FinancialSysUpdate.

Two-tier notification system:
  - Alert Channel:   Immediate per-error notifications during processing
  - Summary Channel: End-of-run summary with all results

Channels are configured per client in MasterConfig (Clients tab):
  - Alert Channel:   e.g. "eMail"
  - Summary Channel: e.g. "Google Chat, Asana"

Comma-separated values route to multiple destinations.

Supported channel types:
  - Google Chat  → POST to webhook URL (from Notifications tab, digested to keyring)
  - Slack        → POST to Slack API (bot token + channel ID from Notifications tab)
  - eMail        → Gmail API (to address from Notifications tab)
"""

import base64
from datetime import datetime
from email.mime.text import MIMEText
from typing import Optional

import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from config import GOOGLE_SCOPES
from logger_setup import get_logger

logger = get_logger()

# Canonical channel names (case-insensitive matching)
CHANNEL_GOOGLE_CHAT = "google chat"
CHANNEL_SLACK = "slack"
CHANNEL_EMAIL = "email"


class NotificationService:
    """Routes alert and summary messages to configured channels."""

    def __init__(self, client_name: str, notifications_config,
                 google_credentials=None):
        """
        Args:
            client_name: Client key (e.g. "ELW")
            notifications_config: NotificationsConfig dataclass from MasterConfig
            google_credentials: Optional pre-authenticated Google credentials
                (reuse from SheetsService to avoid scope issues)
        """
        self.client_name = client_name
        self._config = notifications_config
        self._enabled = notifications_config.active
        self._google_credentials = google_credentials

        # Parse comma-separated channel lists
        self._alert_channels = self._parse_channels(
            notifications_config.alert_channel,
        )
        self._summary_channels = self._parse_channels(
            notifications_config.summary_channel,
        )

        # Lazy-loaded Gmail service
        self._gmail_service = None

        if not self._enabled:
            logger.info("Notifications disabled for this client")
        elif self._alert_channels or self._summary_channels:
            if self._alert_channels:
                logger.info(f"Alert channels: {', '.join(self._alert_channels)}")
            if self._summary_channels:
                logger.info(f"Summary channels: {', '.join(self._summary_channels)}")

    @staticmethod
    def _parse_channels(channel_str: str) -> list[str]:
        """Parse comma-separated channel string into normalized list."""
        if not channel_str or not channel_str.strip():
            return []
        return [ch.strip().lower() for ch in channel_str.split(",") if ch.strip()]

    # ── Public API ──

    def send_alert(self, message: str) -> None:
        """Send an immediate alert to all configured alert channels.

        Used for per-error notifications during processing.
        Failures are logged but never raise — notifications must not
        break the main processing flow.
        """
        if not self._enabled or not self._alert_channels:
            return
        full_message = f"⚠ {self.client_name} Alert\n{message}"
        self._dispatch(self._alert_channels, full_message)

    def send_summary(self, results: dict, year: int,
                      verification_text: str = "") -> None:
        """Send end-of-run summary to all configured summary channels.

        Args:
            results: Dict from ReportProcessor.process_all_reports()
            year: Report year
            verification_text: Optional verification summary to append
        """
        if not self._enabled or not self._summary_channels:
            return
        message = self._build_summary(results, year)
        if verification_text:
            message += "\n" + verification_text
        self._dispatch(self._summary_channels, message)

    # ── Dispatch ──

    def _dispatch(self, channels: list[str], message: str) -> None:
        """Route a message to each channel in the list."""
        for channel in channels:
            try:
                if channel == CHANNEL_GOOGLE_CHAT:
                    self._send_google_chat(message)
                elif channel == CHANNEL_SLACK:
                    self._send_slack(message)
                elif channel == CHANNEL_EMAIL:
                    self._send_email(message)
                else:
                    logger.warning(f"Unknown notification channel: {channel}")
            except Exception as e:
                logger.error(f"Notification failed ({channel}): {e}")

    # ── Channel Senders ──

    def _send_google_chat(self, message: str) -> None:
        """Post to Google Chat via webhook."""
        webhook_url = self._config.google_chat_webhook
        if not webhook_url:
            logger.warning("Google Chat channel configured but no webhook URL")
            return
        response = requests.post(
            webhook_url, json={"text": message}, timeout=10,
        )
        response.raise_for_status()
        logger.info("Google Chat notification sent")

    def _send_slack(self, message: str) -> None:
        """Post to Slack via Bot API."""
        bot_token = self._config.slack_bot_token
        channel_id = self._config.slack_channel_id
        if not bot_token or not channel_id:
            logger.warning("Slack channel configured but missing bot token or channel ID")
            return
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {bot_token}",
                "Content-Type": "application/json",
            },
            json={"channel": channel_id, "text": message},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logger.error(f"Slack API error: {data.get('error', 'unknown')}")
        else:
            logger.info("Slack notification sent")

    def _send_email(self, message: str) -> None:
        """Send via Gmail API using BosOpt credentials."""
        recipient = self._config.email
        if not recipient:
            logger.warning("eMail channel configured but no email address")
            return

        gmail = self._get_gmail_service()
        if not gmail:
            logger.error("Could not authenticate Gmail — skipping email notification")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        subject = f"FinancialSysUpdate — {self.client_name} — {timestamp}"

        mime = MIMEText(message)
        mime["to"] = recipient
        mime["subject"] = subject
        raw = base64.urlsafe_b64encode(mime.as_bytes()).decode("utf-8")

        gmail.users().messages().send(
            userId="me", body={"raw": raw},
        ).execute()
        logger.info(f"Email notification sent to {recipient}")

    def _get_gmail_service(self):
        """Lazy-load Gmail API service.

        Uses pre-authenticated Google credentials from SheetsService when
        available (avoids scope issues where a token refresh drops gmail.send).
        Falls back to loading BosOpt token from disk.
        """
        if self._gmail_service:
            return self._gmail_service

        # Prefer shared credentials from SheetsService — they already have
        # both spreadsheets + gmail.send scopes granted in the access token.
        if self._google_credentials:
            self._gmail_service = build(
                "gmail", "v1", credentials=self._google_credentials,
            )
            logger.info("Gmail service built from shared Google credentials")
            return self._gmail_service

        # Fallback: load from BosOpt token file
        from pathlib import Path
        shared = Path(__file__).parent.parent.parent.parent / "_shared_config"
        token_path = shared / "clients" / "BosOpt" / "token.json"

        creds = None
        if token_path.exists():
            creds = Credentials.from_authorized_user_file(
                str(token_path), GOOGLE_SCOPES,
            )
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                import json
                token_data = {
                    "token": creds.token,
                    "refresh_token": creds.refresh_token,
                    "token_uri": creds.token_uri,
                    "client_id": creds.client_id,
                    "client_secret": creds.client_secret,
                    "scopes": list(GOOGLE_SCOPES),
                }
                token_path.write_text(json.dumps(token_data, indent=2))
            else:
                logger.error(
                    "Gmail token missing or invalid — cannot send email. "
                    "Re-authenticate BosOpt with gmail.send scope."
                )
                return None

        self._gmail_service = build("gmail", "v1", credentials=creds)
        return self._gmail_service

    # ── Message Formatting ──

    def _build_summary(self, results: dict, year: int) -> str:
        """Build a formatted summary message from processing results."""
        # Separate row-change flags from report results
        row_changes = {
            k: v for k, v in results.items() if k.startswith("_row_change_")
        }
        report_results = {
            k: v for k, v in results.items() if not k.startswith("_row_change_")
        }

        success_count = sum(
            1 for r in report_results.values() if r.get("status") == "success"
        )
        error_count = sum(
            1 for r in report_results.values() if r.get("status") == "error"
        )

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [
            f"📊 FinancialSysUpdate — {self.client_name}",
            f"Run: {timestamp}  |  Year: {year}",
            f"Result: {success_count} succeeded, {error_count} failed",
            "",
        ]

        for report, result in report_results.items():
            status = result.get("status", "unknown")
            rows = result.get("rows", 0)
            error = result.get("error", "")
            if status == "success":
                lines.append(f"  ✓ {report}: {rows} rows")
            else:
                lines.append(f"  ✗ {report}: {error}")

        if row_changes:
            lines.append("")
            lines.append("⚠ Row changes detected:")
            for change_key, info in row_changes.items():
                tab = info.get("tab", "")
                added = info.get("rows_added", 0)
                lines.append(f"  {tab}: +{added} row(s) — verify formulas")

        return "\n".join(lines)
