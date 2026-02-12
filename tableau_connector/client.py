# tableau_admissions_report/tableau_connector/client.py
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry # Corrected import path for Retry
import logging
from typing import List, Dict, Any, Optional, Tuple
import urllib.parse 
import time # For potential manual backoff if needed, though Retry handles it

# Get a logger for this module
logger = logging.getLogger(__name__)

class TableauAPIError(Exception):
    """Custom exception for Tableau API errors."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

    def __str__(self):
        return f"{super().__str__()} (Status Code: {self.status_code}, Response: {self.response_text})"


class TableauClient:
    """
    Client for interacting with the Tableau Server REST API.
    Handles authentication, and fetching workbooks, views, and view data.
    Includes retry mechanism for network requests.
    """

    def __init__(self, server_url: str, site_name: str, token_name: str, token_secret: str, api_version: str,
                 connect_timeout: float = 10.0, read_timeout: float = 30.0,
                 total_retries: int = 3, backoff_factor: float = 0.5):
        """
        Initializes the TableauClient.

        Args:
            server_url (str): The base URL of the Tableau server.
            site_name (str): The contentUrl of the Tableau site.
            token_name (str): The name of the Personal Access Token.
            token_secret (str): The secret value of the Personal Access Token.
            api_version (str): The Tableau API version to use.
            connect_timeout (float): Timeout in seconds for connecting to the server.
            read_timeout (float): Timeout in seconds for reading data from the server.
            total_retries (int): Total number of retries to allow.
            backoff_factor (float): A backoff factor to apply between attempts after the second try.
                                   (e.g., 0.5 means 0s, 1s, 2s, 4s, ... delays)
        """
        self.server_url = server_url.rstrip('/')
        self.site_name = site_name
        self.token_name = token_name
        self.token_secret = token_secret
        self.api_version = api_version
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        self.auth_token: Optional[str] = None
        self.site_id: Optional[str] = None
        self.user_id: Optional[str] = None

        self.base_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Setup requests Session with retry mechanism
        self.session = requests.Session()
        retry_strategy = Retry(
            total=total_retries,
            status_forcelist=[429, 500, 502, 503, 504], # Retry on these HTTP status codes
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"], # Retry for all common methods
            backoff_factor=backoff_factor,
            # Respect Retry-After header from server if present
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter) # Though Tableau should be HTTPS

        logger.info(f"TableauClient initialized for server: {self.server_url}, site: {self.site_name}")
        logger.info(f"Request retries configured: total={total_retries}, backoff_factor={backoff_factor}")
        logger.info(f"Request timeouts: connect={connect_timeout}s, read={read_timeout}s")


    def _make_api_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Internal helper method to make requests to the Tableau API using the session.
        """
        full_url = f"{self.server_url}/api/{self.api_version}/{endpoint}"
        
        current_headers = self.base_headers.copy()
        if 'headers' in kwargs:
            custom_headers = kwargs.pop('headers')
            if custom_headers is not None:
                 current_headers.update(custom_headers)

        final_headers = {k: v for k, v in current_headers.items() if v is not None}

        if self.auth_token:
            final_headers["X-Tableau-Auth"] = self.auth_token
        
        # Add explicit timeouts to the request
        if 'timeout' not in kwargs: # If timeout not already specified in call
            kwargs['timeout'] = (self.connect_timeout, self.read_timeout)

        logger.debug(f"Making API request: {method} {full_url}")
        logger.debug(f"Request Headers: {final_headers}")
        if 'json' in kwargs:
            logger.debug(f"Request JSON Payload: {kwargs['json']}")

        try:
            # Use the session object for the request
            response = self.session.request(method, full_url, headers=final_headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as http_err:
            # This will catch 4xx and 5xx errors after retries are exhausted
            logger.error(f"HTTP error during API request to {full_url} after retries: {http_err.response.status_code} - {http_err.response.text}")
            raise TableauAPIError(
                f"Tableau API HTTP error for {method} {endpoint}",
                status_code=http_err.response.status_code,
                response_text=http_err.response.text
            ) from http_err
        except requests.exceptions.ConnectionError as conn_err:
            # This catches DNS failures, refused connections etc., after retries
            logger.error(f"Connection error during API request to {full_url} after retries: {conn_err}")
            # The conn_err string often contains the root cause like 'getaddrinfo failed'
            raise TableauAPIError(f"Tableau API request failed for {method} {endpoint}: {conn_err}") from conn_err
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout error during API request to {full_url} after retries: {timeout_err}")
            raise TableauAPIError(f"Tableau API request timed out for {method} {endpoint}: {timeout_err}") from timeout_err
        except requests.exceptions.RequestException as req_err:
            # Catch any other requests-related errors
            logger.error(f"Request exception during API request to {full_url} after retries: {req_err}")
            raise TableauAPIError(f"Tableau API request failed for {method} {endpoint}: {req_err}") from req_err

    def authenticate(self) -> None:
        """
        Authenticates with the Tableau Server using a Personal Access Token.
        """
        logger.info(f"Attempting authentication for token name: '{self.token_name}' on site: '{self.site_name}'...")
        auth_payload = {
            "credentials": {
                "personalAccessTokenName": self.token_name,
                "personalAccessTokenSecret": self.token_secret,
                "site": {"contentUrl": self.site_name}
            }
        }
        try:
            response = self._make_api_request("POST", "auth/signin", json=auth_payload)
            response_data = response.json()
            
            credentials = response_data.get("credentials", {})
            self.auth_token = credentials.get("token")
            self.site_id = credentials.get("site", {}).get("id")
            self.user_id = credentials.get("user", {}).get("id")

            if not all([self.auth_token, self.site_id]):
                logger.error("Authentication response missing critical credentials (token or siteId).")
                raise TableauAPIError("Authentication failed: Incomplete credentials received.", response_text=response.text)

            logger.info("Authentication successful!")
            logger.debug(f"Auth Token (first 10 chars): {self.auth_token[:10]}...")
            logger.debug(f"Site ID: {self.site_id}, User ID: {self.user_id}")
        except TableauAPIError as e:
            logger.error(f"Authentication failed: {e}")
            raise

    def sign_out(self) -> None:
        """
        Signs out from the Tableau Server, invalidating the current session token.
        """
        if not self.auth_token:
            logger.info("Not authenticated or already signed out. Skipping sign out.")
            return

        logger.info("Attempting to sign out from Tableau Server...")
        try:
            self._make_api_request("POST", "auth/signout")
            logger.info("Successfully signed out from Tableau Server.")
        except TableauAPIError as e:
            logger.warning(f"Sign out attempt failed or was not necessary: {e}")
        finally:
            self.auth_token = None
            self.site_id = None
            self.user_id = None

    def get_workbooks_for_user(self) -> List[Dict[str, Any]]:
        """
        Fetches a list of workbooks accessible to the authenticated user.
        """
        if not self.auth_token or not self.site_id or not self.user_id:
            raise TableauAPIError("Authentication required. Call authenticate() first.", status_code=401)
        
        logger.info(f"Fetching workbooks for user ID: {self.user_id} on site ID: {self.site_id}")
        endpoint = f"sites/{self.site_id}/users/{self.user_id}/workbooks"
        response = self._make_api_request("GET", endpoint)
        return response.json().get("workbooks", {}).get("workbook", [])

    def find_matching_workbooks(self, project_name: str, name_contains_filter: str) -> List[Dict[str, Any]]:
        """
        Filters the user's workbooks to find those matching a specific project name
        and containing a specific substring in their name.
        """
        logger.info(f"Searching for workbooks in project '{project_name}' with name containing '{name_contains_filter}'...")
        all_workbooks = self.get_workbooks_for_user()
        
        matching_workbooks = [
            wb for wb in all_workbooks
            if wb.get("project", {}).get("name") == project_name and \
               name_contains_filter in wb.get("name", "")
        ]

        if matching_workbooks:
            logger.info(f"Found {len(matching_workbooks)} matching workbooks:")
            for wb in matching_workbooks:
                logger.debug(f"  - Name: {wb.get('name')}, ID: {wb.get('id')}")
        else:
            logger.warning(f"No workbooks found matching criteria: project='{project_name}', name_contains='{name_contains_filter}'.")
        return matching_workbooks

    def get_views_for_workbook(self, workbook_id: str) -> List[Dict[str, Any]]:
        """
        Fetches a list of views (sheets, dashboards) within a specific workbook.
        """
        if not self.auth_token or not self.site_id:
            raise TableauAPIError("Authentication required. Call authenticate() first.", status_code=401)

        logger.info(f"Fetching views for workbook ID: {workbook_id}")
        endpoint = f"sites/{self.site_id}/workbooks/{workbook_id}/views"
        response = self._make_api_request("GET", endpoint)
        return response.json().get("views", {}).get("view", [])

    def find_matching_views(self, workbook_id: str, target_view_url_names: List[str]) -> List[Dict[str, Any]]:
        """
        Finds views within a workbook that match a list of target viewUrlNames.
        """
        logger.info(f"Searching for views matching URL names: {target_view_url_names} in workbook ID: {workbook_id}")
        all_views = self.get_views_for_workbook(workbook_id)
        
        matching_views_list = []
        for view in all_views:
            view_url_name = view.get("viewUrlName", "")
            view_name = view.get("name", "") 
            if view_url_name in target_view_url_names or view_name in target_view_url_names:
                 matching_views_list.append(view)
        
        if matching_views_list:
            logger.info(f"Found {len(matching_views_list)} matching views:")
            for v in matching_views_list:
                logger.debug(f"  - Name: {v.get('name')}, URL Name: {v.get('viewUrlName')}, ID: {v.get('id')}")
        else:
            logger.warning(f"No views found matching URL names: {target_view_url_names} in workbook ID '{workbook_id}'.")
        return matching_views_list

    def get_view_data_csv(self, view_id: str, filter_name: Optional[str] = None, filter_values: Optional[List[str]] = None) -> bytes:
        """
        Downloads the data for a specific view as CSV content (bytes).
        """
        if not self.auth_token or not self.site_id:
            raise TableauAPIError("Authentication required. Call authenticate() first.", status_code=401)
        
        param_list = []

        if filter_name and filter_values:
            encoded_filter_name = urllib.parse.quote(filter_name)
            encoded_filter_values = ','.join(urllib.parse.quote(v) for v in filter_values)
            filter_param = f"vf_{encoded_filter_name}={encoded_filter_values}"
            param_list.append(filter_param)
            logger.info(f"Fetching data for view ID: {view_id} with filter: {filter_name}={filter_values}")
        else:
            logger.info(f"Fetching data for view ID: {view_id} (no filters applied through this method).")

        static_params = ["pageType=actual", "orientation=portrait", "maxRowsPerPage=100000"]
        param_list.extend(static_params)

        query_params = "&".join(param_list)
        endpoint = f"sites/{self.site_id}/views/{view_id}/data?{query_params}"
        
        csv_download_headers = {
            "Accept": "text/csv, */*;q=0.8", 
            "Content-Type": None 
        }
        
        response = self._make_api_request("GET", endpoint, headers=csv_download_headers)
        logger.info(f"Successfully fetched CSV data for view ID: {view_id}, size: {len(response.content)} bytes.")
        return response.content