import json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict

class PowerBIClient:
    def __init__(self, token: str, workspace_id: str, dataset_id: str, timeout: int = 90):
        self.token = token
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.session = self._create_session()
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
    
    def _create_session(self) -> requests.Session:
        s = requests.Session()
        retries = Retry(total=3, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
        s.mount("https://", HTTPAdapter(max_retries=retries))
        return s
    
    def execute_dax_query(self, dax_query: str, timeout: int = None) -> List[Dict]:
        url = f"{self.base_url}/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }
        
        query_timeout = timeout or self.timeout
        logging.info(f"Executing DAX query on dataset {self.dataset_id}...")
        try:
            resp = self.session.post(url, headers=headers, json=payload, timeout=self.timeout)
            resp.raise_for_status()
        except requests.Timeout:
            logging.error(f"Timeout! Query execution took more than {query_timeout}s")
            raise RuntimeError(f"Power BI API timeout after {query_timeout}s - dataset maybe too large")
        except requests.HTTPError as e:
            logging.error("Power BI API error: %s", resp.text[:2000])
            logging.error("Status code: %d", resp.status_code)
            raise
        
        result = resp.json()
        try:
            rows = result["results"][0]["tables"][0]["rows"]
            logging.info(f"✅ Retrieved {len(rows)} rows from Power BI")
        except (KeyError, IndexError, TypeError) as e:
            logging.exception("Unexpected Power BI response: %s", json.dumps(result)[:1000])
            raise RuntimeError("Unexpected Power BI response shape") from e
        
        return rows
    
    def close(self):
        self.session.close()
        logging.info("Power BI client closed")