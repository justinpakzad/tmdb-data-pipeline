import requests
from src.utils.utils import get_logger


class TMDBClient:
    def __init__(self, api_key: str) -> None:
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        self.base_url = "https://api.themoviedb.org/3"
        self.session = self.create_session()
        self.logger = get_logger(__name__)

    def create_session(self) -> requests.Session:
        session = requests.session()
        session.headers.update(self.headers)
        return session

    def get(self, endpoint: str, params: dict) -> dict:
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url=url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(str(e))
            return

    def get_paginated(self, endpoint: str, params: dict) -> list[dict]:
        data = []
        while True:
            response = self.get(endpoint, params)
            if params.get("page") == 500:
                break
            data.extend(response)
            params["page"] += 1
        return data
