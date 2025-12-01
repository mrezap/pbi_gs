import logging
from typing import Sequence, List
import gspread

class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str, gs_credentials_path: str):
        self.spreadsheet_id = spreadsheet_id
        self.client = gspread.service_account(filename=gs_credentials_path)
        logging.info(f"Google Sheets client initialized")
    
    def push_rows(self, worksheet_name: str, header: Sequence[str], rows: List[List]):
        """Push data dengan header ke worksheet."""
        sh = self.client.open_by_key(self.spreadsheet_id)
        ws = sh.worksheet(worksheet_name)
        
        payload = [list(header)] + rows
        
        logging.info(f"Pushing {len(rows)} rows to worksheet '{worksheet_name}'...")
        try:
            ws.clear()
            ws.update("A1", payload, value_input_option="USER_ENTERED")
            logging.info(f"Successfully pushed {len(rows)} rows to Google Sheets")
        except Exception as e:
            logging.error("Failed to update Google Sheet: %s", str(e))
            raise
    
    def push_log(self, worksheet_name: str, log_entry: dict):
        """Push single log entry ke worksheet."""
        sh = self.client.open_by_key(self.spreadsheet_id)
        
        # Create worksheet if not exists
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=10)
            # Add header
            header = ["timestamp", "status", "script", "rows_processed", "message"]
            ws.update("A1", [header], value_input_option="USER_ENTERED")
        
        # Append log entry
        row = [
            log_entry.get("timestamp"),
            log_entry.get("status"),
            log_entry.get("script"),
            log_entry.get("rows_processed", ""),
            log_entry.get("message", "")
        ]
        
        logging.info(f"Logging entry to '{worksheet_name}': {log_entry['status']}")
        try:
            ws.append_row(row, value_input_option="USER_ENTERED")
        except Exception as e:
            logging.error("Failed to append log: %s", str(e))
            raise