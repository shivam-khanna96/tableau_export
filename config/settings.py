# tableau_admissions_report/config/settings.py
import os
import sys
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# --- Helper: Directory Management (DRY Principle) ---
def _ensure_directory_exists(dir_path: str, description: str) -> None:
    """
    Checks if a directory exists, and creates it if not.
    Fails fast if permissions prevent creation.
    """
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path, exist_ok=True)
            print(f"Created {description} directory at: {dir_path}")
        except OSError as e:
            # Critical error: If we can't create directories, the app cannot function.
            print(f"CRITICAL ERROR: Failed to create {description} directory at {dir_path}: {e}", file=sys.stderr)
            raise

# --- Project Root & Date ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TODAY_STR = datetime.today().strftime("%Y-%m-%d")

# --- 1. Output Directory Setup ---
DEFAULT_OUTPUT_DIR = "output_reports"
OUTPUT_DIR_NAME = os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR)
OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, OUTPUT_DIR_NAME)

_ensure_directory_exists(OUTPUT_DIR_PATH, "Output Reports")

OUTPUT_EXCEL_FILENAME = f"Admissions Report {TODAY_STR}.xlsx"
OUTPUT_EXCEL_FULL_PATH = os.path.join(OUTPUT_DIR_PATH, OUTPUT_EXCEL_FILENAME)

# --- 2. Logging Directory Setup (New) ---
DEFAULT_LOGS_DIR = "logs"
LOGS_DIR_NAME = os.getenv("LOGS_DIR", DEFAULT_LOGS_DIR)
LOGS_DIR_PATH = os.path.join(PROJECT_ROOT, LOGS_DIR_NAME)

_ensure_directory_exists(LOGS_DIR_PATH, "Logs")

# Log file name matches the Report name, but with .log extension
LOG_FILENAME = f"Admissions Report {TODAY_STR}.log"
LOG_FILE_FULL_PATH = os.path.join(LOGS_DIR_PATH, LOG_FILENAME)


# --- Tableau Configuration ---
TABLEAU_SERVER = os.getenv("TABLEAU_SERVER")
TABLEAU_SITE = os.getenv("TABLEAU_SITE")
TABLEAU_TOKEN_NAME = os.getenv("TABLEAU_TOKEN_NAME")
TABLEAU_TOKEN_SECRET = os.getenv("TABLEAU_TOKEN_SECRET")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.19")

if not all([TABLEAU_SERVER, TABLEAU_SITE, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_SECRET]):
    raise ValueError("Missing essential Tableau configuration in .env.")


# --- Graph API Email Configuration ---
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_SENDER_EMAIL = os.getenv("GRAPH_SENDER_EMAIL")

if not all([GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_TENANT_ID, GRAPH_SENDER_EMAIL]):
    print("WARNING: Graph API credentials missing. Email sending will fail.")


# --- Report Specific Configuration ---
TARGET_PROJECT_NAME = "Admissions Pipeline"
TARGET_WORKBOOK_NAME_CONTAINS = "Student_Lifecycle_Pipeline"
TARGET_VIEW_URL_NAMES = [
    "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable",
    "PowerCampusApplicantDownload",
    "SubmittedApplicantStatusDetailed"
]

VIEW_URL_NAME_TO_SHEET_NAME_MAP = {
    "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable": "Progress Report",
    "PowerCampusApplicantDownload": "Raw Data",
    "SubmittedApplicantStatusDetailed": "Application Status Breakdown"
}

VIEW_FILTER_NAME = "Application Term"
VIEW_FILTER_VALUES_MULTI_TERM = ["FALL 2025", "SPRING 2026", "SUMMER 2026"]
VIEW_FILTER_VALUES_SUMMER_ONLY = ["SUMMER 2026"]


# --- Email Content ---
EMAIL_RECIPIENTS_STR = os.getenv("EMAIL_RECIPIENTS", "")
EMAIL_RECIPIENTS_LIST = [email.strip() for email in EMAIL_RECIPIENTS_STR.split(';') if email.strip()]

EMAIL_SUBJECT_PREFIX = "Weekly Admissions Report"
EMAIL_BODY_GREETING = """Hi team,
Please find the latest admissions report attached to this email."""
EMAIL_BODY_SIGNATURE = """Thanks,
Automated Report System
(Developed by Shivam Khanna)"""


# --- Data Processing Constants ---
# Progress Report
PROGRESS_REPORT_VIEW_URL_NAME = "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable"
PROGRESS_REPORT_DROP_COLUMNS = ['ApplicationTerm Order']
PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING = 'All'
PROGRESS_REPORT_PIVOT_INDEX_COLUMNS = ["Application Term", "Program", "CURRICULUM", "DEGREE"]
PROGRESS_REPORT_PIVOT_AGG_COLUMN = "Measure Names"
PROGRESS_REPORT_PIVOT_VALUES_COLUMN = "Measure Values"
PROGRESS_REPORT_FINAL_COLUMN_ORDER = [
    "Application Term", "Program", "CURRICULUM", "DEGREE",
    "Submitted Applicants", "Qualified Applicants",
    "Admitted Applicants", "Wait Listed", "Deposited"
]
PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION = [
    "Submitted Applicants", "Qualified Applicants",
    "Admitted Applicants", "Wait Listed", "Deposited"
]
PROGRESS_REPORT_SUBTOTAL_COLUMNS_TO_AGGREGATE = PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION

# Admit Breakdown
ADMIT_BREAKDOWN_VIEW_URL_NAME = "SubmittedApplicantStatusDetailed"
ADMIT_BREAKDOWN_DROP_COLUMNS = ['ApplicationTerm Order']
ADMIT_BREAKDOWN_REMOVE_ROW_IF_CONTAINS_STRING = 'All'
ADMIT_BREAKDOWN_PIVOT_INDEX_COLUMNS = ["Application Term", "Program", "CURRICULUM", "DEGREE"]
ADMIT_BREAKDOWN_PIVOT_AGG_COLUMN = "Measure Names"
ADMIT_BREAKDOWN_PIVOT_VALUES_COLUMN = "Measure Values"
ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER = [
    "Application Term", "Program", "CURRICULUM", "DEGREE",
    "Submitted Applicants",
    "Admitted and Deposited",
    "Admitted with Deposit, Deferred to Future Term",
    "Admitted Without Deposit",
    "Admitted, Not Coming After Deposit",
    "Admitted, Not Coming, No Deposit",
    "Withdrawn Before Decision",
    "Withdrawn After Registration",
    "Under Admission+Faculty Review/In Process"
]
ADMIT_BREAKDOWN_NUMERIC_COLUMNS_FOR_INT_CONVERSION = [
    "Submitted Applicants",
    "Admitted and Deposited",
    "Admitted with Deposit, Deferred to Future Term",
    "Admitted Without Deposit",
    "Admitted, Not Coming After Deposit",
    "Admitted, Not Coming, No Deposit",
    "Withdrawn Before Decision",
    "Withdrawn After Registration",
    "Under Admission+Faculty Review/In Process"
]
ADMIT_BREAKDOWN_SUBTOTAL_COLUMNS_TO_AGGREGATE = ADMIT_BREAKDOWN_NUMERIC_COLUMNS_FOR_INT_CONVERSION

# Raw Data
RAW_DATA_VIEW_URL_NAME = "PowerCampusApplicantDownload"
RAW_DATA_DROP_COLUMNS = ['Blank', 'Month, Day, Year of Data Refresh Date', 'Index', 'Count of FIRST_NAME']
RAW_DATA_FINAL_COLUMN_SELECTION_ORDER = [
    'PEOPLE_CODE_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
    'Application Term', 'Program', 'CURRICULUM', 'DEGREE', 'Campus',
    'ACADEMIC_SESSION', 'APP_DECISION', 'Submitted Applicant Decision',
    'APP_STATUS', 'Submitted Applicant Status', 'ENROLL_SEPARATION',
    'APPLICATION_DATE', 'ACADEMIC_FLAG'
]


# --- Logging Configuration ---
LOGGING_LEVEL = "INFO" # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# --- Excel Formatting ---
EXCEL_SHEET_NAME_PROGRESS_REPORT = "Progress Report"
EXCEL_SHEET_NAME_ADMIT_BREAKDOWN = "Application Status Breakdown"
EXCEL_FONT_BOLD = True
EXCEL_ALIGNMENT_CENTER = {'horizontal': 'center', 'vertical': 'center'}
EXCEL_FILL_ALT_ROW = {"fill_type": "solid", "start_color": "F2F2F2", "end_color": "F2F2F2"}
EXCEL_FILL_TOTAL_ROW = {"fill_type": "solid", "start_color": "D9D9D9", "end_color": "D9D9D9"}
EXCEL_FILL_GRAND_TOTAL_ROW = {"fill_type": "solid", "start_color": "C9C9C9", "end_color": "C9C9C9"}
EXCEL_BORDER_SIDE_THIN_WHITE = {"style": "thin", "color": "FFFFFF"}
EXCEL_BORDER_SIDE_MEDIUM_BLACK = {"style": "medium", "color": "000000"}