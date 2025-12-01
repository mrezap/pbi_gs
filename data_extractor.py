import logging
import datetime
from typing import Sequence
from config import Config
from auth import PowerBIAuth
from powerbi import PowerBIClient
from sheets import GoogleSheetsClient

class DataExtractor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.auth = PowerBIAuth(cfg)
        self.token = None
        self.pbi_client = None
        self.sheets_client = None
    
    def __enter__(self):
        self.token = self.auth.get_access_token()
        self.pbi_client = PowerBIClient(self.token, self.cfg.workspace_id, self.cfg.dataset_id)
        self.sheets_client = GoogleSheetsClient(self.cfg.spreadsheet_id, self.cfg.gs_credentials_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pbi_client:
            self.pbi_client.close()
        if exc_type:
            logging.error("Error occurred: %s", exc_val)
    
    def extract_and_push(self, dax_query: str, worksheet_name: str, header: Sequence[str]) -> int:
        """Execute DAX query and push data to Google Sheets."""
        rows = self.pbi_client.execute_dax_query(dax_query)
        
        if not rows:
            logging.warning("No data returned from Power BI query")
            return 0
        
        formatted = [list(r.values()) for r in rows]
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with_ts = [r + [now] for r in formatted]
        
        header_with_ts = list(header) + ["update_at"]
        self.sheets_client.push_rows(worksheet_name, header_with_ts, with_ts)
        
        return len(with_ts)