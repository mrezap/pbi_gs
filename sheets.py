import logging
from typing import Sequence, Optional
import gspread

class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str, gs_path: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        self.client = self._create_client(gs_path)
    
    def _create_client(self, gs_path: Optional[str]):
        if gs_path:
            logging.info(f"Using Google credentials from file: {gs_path}")
            return gspread.service_account(filename=gs_path)
        else:
            logging.info("Using default Google credentials")
            return gspread.service_account()
    
    def push_rows(self, worksheet_name: str, header: Sequence[str], rows: Sequence[Sequence]):
        sh = self.client.open_by_key(self.spreadsheet_id)
        ws = sh.worksheet(worksheet_name)
        payload = [list(header)] + [list(r) for r in rows]
        
        logging.info(f"Pushing {len(rows)} rows to worksheet '{worksheet_name}'...")
        try:
            ws.clear()
            ws.update("A1", payload, value_input_option="USER_ENTERED")
            logging.info(f"✅ Successfully pushed {len(rows)} rows to Google Sheets")
        except Exception as e:
            logging.exception("Failed to update Google Sheet: %s", str(e))
            raise