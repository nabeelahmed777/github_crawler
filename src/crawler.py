import time
from typing import List, Optional
from datetime import datetime
from src.models import Repository
from src.github_api import GitHubAPI
from src.database import DatabaseManager


class RepositoryCrawler:
    """High-performance repository crawler with clean architecture"""

    def __init__(self, github_token: str, db_config: dict):
        self.github_api = GitHubAPI(github_token)
        self.db_manager = DatabaseManager(db_config)
        self.processed_count = 0

    def fetch_repository_batch(
        self, cursor: Optional[str] = None, batch_size: int = 100
    ) -> tuple:
        """Fetch batch of repositories efficiently"""
        query = """
        query($cursor: String, $batchSize: Int!) {
          search(
            query: "stars:>1 sort:stars-desc"
            type: REPOSITORY
            first: $batchSize
            after: $cursor
          ) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                id
                name
                owner { login }
                nameWithOwner
                stargazerCount
                url
                description
                primaryLanguage { name }
                createdAt
                updatedAt
              }
            }
          }
        }
        """

        variables = {"cursor": cursor, "batchSize": batch_size}
        data = self.github_api.execute_query(query, variables)

        if not data or "data" not in data:
            return [], None, False

        search_data = data["data"]["search"]
        repositories = []

        for node in search_data["nodes"]:
            try:
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
                    created_at=datetime.strptime(
                        node["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    updated_at=datetime.strptime(
                        node["updatedAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    crawled_at=datetime.now(),
                )
                repositories.append(repo)
            except (KeyError, ValueError) as e:
                print(f"Skipping invalid repository data: {e}")
                continue

        page_info = search_data["pageInfo"]
        return repositories, page_info["endCursor"], page_info["hasNextPage"]

    def crawl(self, max_repositories: int = 100000, batch_size: int = 100):
        """Main crawling method optimized for speed"""
        print(f" ==> Starting crawl for {max_repositories} repositories")

        # Rate limit check
        rate_limit = self.github_api.get_rate_limit_status()
        if rate_limit:
            print(f" ==? Rate limit: {rate_limit['remaining']}/{rate_limit['limit']}")

        cursor = None
        has_next = True

        while has_next and self.processed_count < max_repositories:
            start_time = time.time()

            repositories, cursor, has_next = self.fetch_repository_batch(
                cursor, batch_size
            )

            if not repositories:
                print(" ==> No repositories fetched, stopping")
                break

            # Bulk insert for better performance
            saved_count = self.db_manager.bulk_upsert_repositories(repositories)
            self.processed_count += len(repositories)

            batch_time = time.time() - start_time
            repos_per_second = len(repositories) / batch_time if batch_time > 0 else 0

            print(
                f" ==>  Batch: {len(repositories)} repos | "
                f"Total: {self.processed_count} | "
                f"Speed: {repos_per_second:.1f} repos/sec | "
                f"Saved: {saved_count}"
            )

            # Adaptive delay based on rate limit
            time.sleep(0.5)  # Reduced delay for faster crawling

        print(f" == > Crawl completed: {self.processed_count} repositories processed")
