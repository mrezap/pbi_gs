import logging
import requests
from config import Config

class PowerBIAuth:
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def get_access_token(self) -> str:
        """Get token using password grant flow (manual requests - sama seperti file lama)."""
        if not (self.cfg.tenant_id and self.cfg.client_id and self.cfg.client_secret and 
                self.cfg.username and self.cfg.password):
            raise RuntimeError("Missing credentials in environment")
        
        url = f'https://login.microsoftonline.com/{self.cfg.tenant_id}/oauth2/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {
            'resource': 'https://analysis.windows.net/powerbi/api',
            'client_id': self.cfg.client_id,
            'client_secret': self.cfg.client_secret,
            'grant_type': 'password',
            'username': self.cfg.username,
            'password': self.cfg.password,
            'scope': 'openid'
        }
        
        logging.info("Acquiring access token...")
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
        except requests.HTTPError as e:
            logging.error("Token acquisition failed: %s", response.text[:500])
            raise RuntimeError("Failed to acquire access token") from e
        
        result = response.json()
        
        if 'error' in result:
            logging.error("OAuth error: %s - %s", result.get('error'), result.get('error_description'))
            raise RuntimeError("Failed to acquire access token")
        
        if "access_token" not in result:
            logging.error("No access token in response: %s", result)
            raise RuntimeError("Failed to acquire access token")
        
        logging.info("✅ Access token acquired successfully")
        return result["access_token"]