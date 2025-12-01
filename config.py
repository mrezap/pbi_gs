import os
import json
import base64
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Optional, Dict

load_dotenv()

@dataclass
class Config:
    tenant_id: str
    client_id: str
    client_secret: str
    username: str
    password: str
    workspace_id: str
    dataset_id: str
    spreadsheet_id: str
    gs_credentials_path: Optional[str]

def load_config() -> Config:
    return Config(
        tenant_id=os.getenv("TENANT_ID", ""),
        client_id=os.getenv("CLIENT_ID", ""),
        client_secret=os.getenv("CLIENT_SECRET", ""),
        username=os.getenv("USERNAME", ""),
        password=os.getenv("PASSWORD", ""),
        workspace_id=os.getenv("WORKSPACE_ID", ""),
        dataset_id=os.getenv("DATASET_ID", ""),
        spreadsheet_id=os.getenv("SPREADSHEET_ID", ""),
        gs_credentials_path=os.getenv("GS_CREDENTIALS_PATH"),
    )