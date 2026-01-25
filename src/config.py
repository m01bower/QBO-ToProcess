"""Configuration constants for QBO ToProcess."""

# Client names
CLIENTS = ["BostonHCP", "LSC", "ELW", "SprayValet", "BosOpt"]

# QuickBooks API Configuration
QBO_AUTH_ENDPOINT = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_ENDPOINT = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE_URL = "https://quickbooks.api.intuit.com/v3/company"
QBO_SANDBOX_API_BASE_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company"

# OAuth scopes for QuickBooks
QBO_SCOPES = ["com.intuit.quickbooks.accounting"]

# Google API Scopes
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

# ToProcess tab configuration
TOPROCESS_TAB_NAME = "ToProcess"
TOPROCESS_COLUMNS = {
    "year_cell": "A1",
    "row_max": "D",
    "col_max": "E",
    "sheet_id": "F",
    "tab_name": "G",
    "starting_cell": "H",
    "temp_tab": "I",
    "new_tab_name": "J",
    "processed_date": "K",
    "qbo_report": "L",
    "report_display": "M",
    "report_basis": "N",
}

# Row/Column max values
MAX_ALL = "*"
MAX_STOP_AT_TOTAL = "T"
MAX_STOP_BEFORE_TOTAL = "-T"

# QBO Report Types (internal names)
QBO_REPORTS = {
    "Balance Sheet": "BalanceSheet",
    "P&L": "ProfitAndLoss",
    "Profit and Loss": "ProfitAndLoss",
    "AR Aging": "AgedReceivables",
    "AR Aging Summary": "AgedReceivablesSummary",
    "Sales by Customer Summary": "CustomerSales",
    "Sales by Product Summary": "ItemSales",
}

# Report display options
REPORT_DISPLAY = {
    "Monthly": "Months",
    "Weekly": "Weeks",
    "Quarterly": "Quarters",
    "Yearly": "Years",
    "Detail": "Detail",
    "Summary": "Summary",
}

# Accounting basis options
REPORT_BASIS = {
    "Cash": "Cash",
    "Accrual": "Accrual",
}

# Config file paths (relative to project root)
CONFIG_DIR = "config"
SETTINGS_FILE = "settings.json"
CLIENTS_FILE = "clients.json"
SERVICE_ACCOUNT_FILE = "service_account.json"
QBO_APP_FILE = "qbo_app.json"
QBO_TOKENS_DIR = "qbo_tokens"
