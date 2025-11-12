import requests
import time
from typing import Optional, Dict, Any
from datetime import datetime


class GitHubAPI:
    """Anti-corruption layer for GitHub GraphQL API"""

    def __init__(self, token: str):
        self.base_url = "https://api.github.com/graphql"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def execute_query(
        self, query: str, variables: dict = None, max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """Execute GraphQL query with retry mechanism"""
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.base_url,
                    json={"query": query, "variables": variables},
                    timeout=30,
                )

                if response.status_code == 200:
                    data = response.json()
                    if "errors" not in data:
                        return data
                    else:
                        print(f"GraphQL errors: {data['errors']}")

                elif response.status_code == 403:  # Rate limit
                    reset_time = int(
                        response.headers.get("X-RateLimit-Reset", time.time() + 60)
                    )
                    wait_time = max(reset_time - time.time(), 0)
                    print(f"Rate limit hit. Waiting {wait_time:.0f} seconds")
                    time.sleep(wait_time + 1)
                    continue

                elif response.status_code >= 500:  # Server errors
                    time.sleep(2**attempt)  # Exponential backoff
                    continue

            except requests.exceptions.Timeout:
                time.sleep(2**attempt)
                continue

        return None

    def get_rate_limit_status(self) -> Optional[Dict[str, Any]]:
        """Check current rate limit status"""
        query = """
        query {
          rateLimit {
            limit
            cost
            remaining
            resetAt
          }
        }
        """
        data = self.execute_query(query)
        return data["data"]["rateLimit"] if data else None
