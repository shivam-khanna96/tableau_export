# tableau_admissions_report/config/settings.py
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from the .env file in the project root
# Assumes .env file is in the parent directory of this config file's directory
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# --- Tableau Configuration ---
TABLEAU_SERVER = os.getenv("TABLEAU_SERVER")
TABLEAU_SITE = os.getenv("TABLEAU_SITE")
TABLEAU_TOKEN_NAME = os.getenv("TABLEAU_TOKEN_NAME")
TABLEAU_TOKEN_SECRET = os.getenv("TABLEAU_TOKEN_SECRET")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.19") # Default if not in .env

# Validate essential Tableau credentials
if not all([TABLEAU_SERVER, TABLEAU_SITE, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_SECRET]):
    raise ValueError(
        "One or more Tableau configuration variables are missing in the .env file. "
        "Please ensure TABLEAU_SERVER, TABLEAU_SITE, TABLEAU_TOKEN_NAME, and TABLEAU_TOKEN_SECRET are set."
    )

# --- Report Specific Configuration ---
# Workbook and View identification
TARGET_PROJECT_NAME = "Admissions Pipeline"
TARGET_WORKBOOK_NAME_CONTAINS = "Student_Lifecycle_Pipeline"
TARGET_VIEW_URL_NAMES = [ # Using viewUrlName for more stable matching
    "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable",
    "PowerCampusApplicantDownload",
    "SubmittedApplicantStatusDetailed"
]

# Mapping from Tableau View URL Name to desired Excel sheet name
VIEW_URL_NAME_TO_SHEET_NAME_MAP = {
    "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable": "Progress Report",
    "PowerCampusApplicantDownload": "Raw Data",
    "SubmittedApplicantStatusDetailed": "Application Status Breakdown"
}

# Filter configuration for Tableau views
VIEW_FILTER_NAME = "Application Term"
# For views that need all three terms (like Progress Report)
VIEW_FILTER_VALUES_MULTI_TERM = ["SUMMER 2025", "FALL 2025", "SPRING 2025"]
# For views that need only Summer 2025 (like Application Status Breakdown)
VIEW_FILTER_VALUES_SUMMER_ONLY = ["FALL 2025"]

# --- Output File Configuration ---
TODAY_STR = datetime.today().strftime("%Y-%m-%d")
# Default output directory if not set in .env
DEFAULT_OUTPUT_DIR = "output_reports" # Relative to project root
OUTPUT_DIR_NAME = os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR)

# Construct absolute path for OUTPUT_DIR relative to project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, OUTPUT_DIR_NAME)

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR_PATH):
    try:
        os.makedirs(OUTPUT_DIR_PATH)
    except OSError as e:
        # Handle error if directory creation fails, e.g., due to permissions
        print(f"Error creating output directory {OUTPUT_DIR_PATH}: {e}")
        # Depending on requirements, you might want to raise the error or exit
        raise

OUTPUT_EXCEL_FILENAME = f"Admissions Report {TODAY_STR}.xlsx"
OUTPUT_EXCEL_FULL_PATH = os.path.join(OUTPUT_DIR_PATH, OUTPUT_EXCEL_FILENAME)


# --- Email Configuration ---
# Recipients are loaded from .env, split by semicolon, and stripped of whitespace
EMAIL_RECIPIENTS_STR = os.getenv("EMAIL_RECIPIENTS", "")
EMAIL_RECIPIENTS_LIST = [email.strip() for email in EMAIL_RECIPIENTS_STR.split(';') if email.strip()]

EMAIL_SUBJECT_PREFIX = "Weekly Admissions Report"
EMAIL_BODY_GREETING = """Hi team,
Please find the latest admissions report attached to this email."""
EMAIL_BODY_SIGNATURE = """Thanks,
Automated Report System
(Developed by Shivam Khanna)"""


# --- Data Processing Constants for 'Progress Report' ---
PROGRESS_REPORT_VIEW_URL_NAME = "Applicants-SubmittedQualifiedAdmittedWaitListedDepositedTable"
PROGRESS_REPORT_DROP_COLUMNS = ['ApplicationTerm Order']
PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING = 'All' # Case-insensitive check
PROGRESS_REPORT_PIVOT_INDEX_COLUMNS = ["Application Term", "Program", "CURRICULUM", "DEGREE"]
PROGRESS_REPORT_PIVOT_AGG_COLUMN = "Measure Names"
PROGRESS_REPORT_PIVOT_VALUES_COLUMN = "Measure Values"
PROGRESS_REPORT_FINAL_COLUMN_ORDER = [
    "Application Term", "Program", "CURRICULUM", "DEGREE",
    "Submitted Applicants", "Qualified Applicants",
    "Admitted Applicants", "Wait Listed", "Deposited", "Enrolled"
]
PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION = [
    "Submitted Applicants", "Qualified Applicants",
    "Admitted Applicants", "Wait Listed", "Deposited", "Enrolled"
]
PROGRESS_REPORT_SUBTOTAL_COLUMNS_TO_AGGREGATE = PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION


# --- Data Processing Constants for 'Application Status Breakdown' Report --- # <<< NEW SECTION
ADMIT_BREAKDOWN_VIEW_URL_NAME = "SubmittedApplicantStatusDetailed"
ADMIT_BREAKDOWN_DROP_COLUMNS = ['ApplicationTerm Order'] # Add any columns to drop if Tableau export includes extras not in the image
ADMIT_BREAKDOWN_REMOVE_ROW_IF_CONTAINS_STRING = 'All' # Assuming similar 'All' rows might exist
ADMIT_BREAKDOWN_PIVOT_INDEX_COLUMNS = ["Application Term", "Program", "CURRICULUM", "DEGREE"]
ADMIT_BREAKDOWN_PIVOT_AGG_COLUMN = "Measure Names" # This assumes the Tableau view is structured similarly to Progress Report's source
ADMIT_BREAKDOWN_PIVOT_VALUES_COLUMN = "Measure Values" # Same assumption as above
ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER = [
    "Application Term", "Program", "CURRICULUM", "DEGREE",
    "Submitted Applicants", # Ensure this matches the exact column name from Tableau export
    "Admitted and Deposited",
    "Admitted with Deposit, Deferred to Future Term",
    "Admitted Without Deposit",
    "Admitted, Not Coming After Deposit",
    "Admitted, Not Coming, No Deposit",
    "Withdrawn Before Decision",
    "Withdrawn After Registration", # Ensure this matches the exact column name from Tableau export
    "Under Admission+Faculty Review/In Process"
]
ADMIT_BREAKDOWN_NUMERIC_COLUMNS_FOR_INT_CONVERSION = [
    "Submitted Applicants", # Ensure this matches the exact column name from Tableau export
    "Admitted and Deposited",
    "Admitted with Deposit, Deferred to Future Term",
    "Admitted Without Deposit",
    "Admitted, Not Coming After Deposit",
    "Admitted, Not Coming, No Deposit",
    "Withdrawn Before Decision",
    "Withdrawn After Registration", # Ensure this matches the exact column name from Tableau export
    "Under Admission+Faculty Review/In Process"
]
ADMIT_BREAKDOWN_SUBTOTAL_COLUMNS_TO_AGGREGATE = ADMIT_BREAKDOWN_NUMERIC_COLUMNS_FOR_INT_CONVERSION


# --- Data Processing Constants for 'Raw Data' Report ---
RAW_DATA_VIEW_URL_NAME = "PowerCampusApplicantDownload"
RAW_DATA_DROP_COLUMNS = ['Blank', 'Month, Day, Year of Data Refresh Date', 'Index', 'Count of FIRST_NAME']
RAW_DATA_FINAL_COLUMN_SELECTION_ORDER = [
    'PEOPLE_CODE_ID', 'FIRST_NAME', 'LAST_NAME', 'Personal_EMAIL', 'SMU_EMAIL',
    'Application Term', 'Program', 'CURRICULUM', 'DEGREE', 'Campus',
    'ACADEMIC_SESSION', 'APP_DECISION', 'Submitted Applicant Decision',
    'APP_STATUS', 'Submitted Applicant Status', 'ENROLL_SEPARATION',
    'APPLICATION_DATE', 'ACADEMIC_FLAG'
]

# --- Logging Configuration (Basic) ---
LOGGING_LEVEL = "INFO" # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# --- Excel Formatting Constants ---
EXCEL_SHEET_NAME_PROGRESS_REPORT = "Progress Report" # Should match value in VIEW_URL_NAME_TO_SHEET_NAME_MAP
EXCEL_SHEET_NAME_ADMIT_BREAKDOWN = "Application Status Breakdown" # Should match value in VIEW_URL_NAME_TO_SHEET_NAME_MAP
EXCEL_FONT_BOLD = True
EXCEL_ALIGNMENT_CENTER = {'horizontal': 'center', 'vertical': 'center'}
EXCEL_FILL_ALT_ROW = {"fill_type": "solid", "start_color": "F2F2F2", "end_color": "F2F2F2"}
EXCEL_FILL_TOTAL_ROW = {"fill_type": "solid", "start_color": "D9D9D9", "end_color": "D9D9D9"}
EXCEL_FILL_GRAND_TOTAL_ROW = {"fill_type": "solid", "start_color": "C9C9C9", "end_color": "C9C9C9"} # Slightly darker
EXCEL_BORDER_SIDE_THIN_WHITE = {"style": "thin", "color": "FFFFFF"}
EXCEL_BORDER_SIDE_MEDIUM_BLACK = {"style": "medium", "color": "000000"}

