import requests
import time
import json
from datetime import datetime
from typing import List, Optional
from config import GITHUB_TOKEN, GRAPHQL_URL, RATE_LIMIT_DELAY
from src.models import Repository
from src.database import DatabaseManager


class GitHubCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Content-Type": "application/json",
            }
        )
        self.db = DatabaseManager()
        self.crawled_count = 0

    def check_authentication(self):
        """Check if GitHub token is working"""
        check_query = """
        query {
          viewer {
            login
          }
          rateLimit {
            limit
            cost
            remaining
            resetAt
          }
        }
        """

        response = self.make_graphql_query(check_query)
        if response and "data" in response:
            print("✅ GitHub authentication successful")
            rate_limit = response["data"]["rateLimit"]
            print(
                f"Rate limit: {rate_limit['remaining']}/{rate_limit['limit']} remaining"
            )
            return True
        else:
            print("❌ GitHub authentication failed")
            return False

    def make_graphql_query(self, query: str, variables: dict = None) -> Optional[dict]:
        """Make GraphQL query to GitHub API with error handling"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    GRAPHQL_URL,
                    json={"query": query, "variables": variables},
                    timeout=30,
                )

                print(f"API Response Status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    if "errors" in data:
                        print(f"GraphQL errors: {data['errors']}")
                        return None
                    return data
                elif response.status_code == 502:
                    print(
                        f"🔁 502 Bad Gateway (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                elif response.status_code == 403:
                    # Rate limit hit
                    reset_time = response.headers.get("X-RateLimit-Reset")
                    if reset_time:
                        wait_time = int(reset_time) - time.time() + 10
                        print(f"⏳ Rate limit hit. Waiting {wait_time:.0f} seconds")
                        time.sleep(max(wait_time, 0))
                        return self.make_graphql_query(query, variables)
                    else:
                        print("Rate limit hit but no reset time provided")
                        return None
                else:
                    print(
                        f"❌ HTTP error {response.status_code}: {response.text[:200]}"
                    )
                    return None

            except requests.exceptions.Timeout:
                print(f"⏰ Request timeout (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2
            except Exception as e:
                print(f"❌ Error making GraphQL query: {e}")
                return None

        print("🔴 All retry attempts failed")
        return None

    def get_popular_repositories(self, cursor: str = None) -> tuple:
        """Get a batch of popular repositories"""
        query = """
        query($cursor: String) {
          search(
            query: "stars:>100"
            type: REPOSITORY
            first: 50
            after: $cursor
          ) {
            repositoryCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                id
                name
                owner {
                  login
                }
                nameWithOwner
                stargazerCount
                url
                description
                primaryLanguage {
                  name
                }
                createdAt
                updatedAt
              }
            }
          }
          rateLimit {
            cost
            remaining
            resetAt
          }
        }
        """

        variables = {"cursor": cursor}
        data = self.make_graphql_query(query, variables)

        if not data:
            return [], None, False

        search_data = data["data"]["search"]
        repositories = []

        # Fix: Use 'nodes' instead of 'edges'
        for node in search_data["nodes"]:
            repo = Repository(
                id=node["id"],
                name=node["name"],
                owner=node["owner"]["login"],
                name_with_owner=node["nameWithOwner"],
                stargazers_count=node["stargazerCount"],
                url=node["url"],
                description=node["description"],
                primary_language=node["primaryLanguage"]["name"]
                if node["primaryLanguage"]
                else None,
                created_at=datetime.strptime(node["createdAt"], "%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime.strptime(node["updatedAt"], "%Y-%m-%dT%H:%M:%SZ"),
                crawled_at=datetime.now(),
            )
            repositories.append(repo)

        page_info = search_data["pageInfo"]
        next_cursor = page_info["endCursor"] if page_info["hasNextPage"] else None

        return repositories, next_cursor, page_info["hasNextPage"]

    def crawl_repositories(self, max_repositories: int = 1000):  # Reduced for testing
        """Main crawling method"""
        print("Starting GitHub repository crawl...")

        # First check authentication
        if not self.check_authentication():
            print("🔴 Cannot proceed without authentication")
            return

        cursor = None
        has_next_page = True

        while has_next_page and self.crawled_count < max_repositories:
            print(f"🔄 Fetching batch {self.crawled_count // 50 + 1}...")

            repositories, cursor, has_next_page = self.get_popular_repositories(cursor)

            if not repositories:
                print("No repositories returned, stopping crawl")
                break

            # Save to database
            successful_saves = 0
            for repo in repositories:
                if self.db.upsert_repository(repo):
                    successful_saves += 1

            self.crawled_count += len(repositories)
            print(
                f"✅ Crawled {self.crawled_count} repositories. Last batch: {len(repositories)} repositories, {successful_saves} saved/updated"
            )

            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)

            # Safety check - don't exceed max
            if self.crawled_count >= max_repositories:
                break

        print(f"🎉 Crawl completed. Total repositories processed: {self.crawled_count}")
        print(f"💾 Total in database: {self.db.get_repository_count()}")

    def close(self):
        """Clean up resources"""
        self.db.close()
