import logging
import datetime
from typing import Sequence, List
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
            return False
    
    def extract_and_push(self, dax_query: str, worksheet_name: str, header: Sequence[str], 
                        log_worksheet: str = None, script_name: str = None) -> int:
        """Execute DAX query, push ke Google Sheets, dan log execution."""
        start_time = datetime.datetime.now()
        rows_count = 0
        status = "SUCCESS"
        message = ""
        
        #query_timeout = timeout or self.timeout
        
        try:
            # Extract from Power BI
            rows = self.pbi_client.execute_dax_query(dax_query, timeout=120)
            
            if not rows:
                logging.warning("No data returned from Power BI query")
                status = "WARNING"
                message = "No data returned from Power BI"
                rows_count = 0
            else:
                # Format rows
                formatted = [list(row.values()) for row in rows]
                
                # Add timestamp
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with_timestamp = [row + [current_time] for row in formatted]
                
                # Push ke sheets
                header_with_ts = list(header) + ["update_at"]
                self.sheets_client.push_rows(worksheet_name, header_with_ts, with_timestamp)
                
                rows_count = len(with_timestamp)
        
        except Exception as e:
            status = "FAILED"
            message = str(e)
            logging.exception("Error during extraction")
        
        finally:
            # Log execution
            if log_worksheet:
                end_time = datetime.datetime.now()
                log_entry = {
                    "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": status,
                    "script": script_name or worksheet_name,
                    "rows_processed": rows_count,
                    "message": message
                }
                
                try:
                    self.sheets_client.push_log(log_worksheet, log_entry)
                    logging.info(f"Log entry pushed: {status}")
                except Exception as e:
                    logging.error(f"Failed to push log: {e}")
        
        return rows_count