import psycopg2
from typing import List
from src.models import Repository


class DatabaseManager:
    """Handles all database operations with proper separation"""

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.connection = self._create_connection()
        self._setup_schema()

    def _create_connection(self):
        """Create database connection - Anti-corruption layer"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            raise ConnectionError(f"Database connection failed: {e}")

    def _setup_schema(self):
        """Initialize database schema"""
        schema_queries = [
            """
            CREATE TABLE IF NOT EXISTS repositories (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                owner VARCHAR(255) NOT NULL,
                name_with_owner VARCHAR(510) NOT NULL,
                stargazers_count INTEGER NOT NULL,
                url VARCHAR(500),
                description TEXT,
                primary_language VARCHAR(100),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_name_with_owner ON repositories(name_with_owner)",
            "CREATE INDEX IF NOT EXISTS idx_stargazers ON repositories(stargazers_count DESC)",
        ]

        with self.connection.cursor() as cursor:
            for query in schema_queries:
                cursor.execute(query)
        self.connection.commit()

    def bulk_upsert_repositories(self, repositories: List[Repository]) -> int:
        """Efficient bulk upsert operation for better performance"""
        query = """
            INSERT INTO repositories 
            (id, name, owner, name_with_owner, stargazers_count, url, description, primary_language, created_at, updated_at, crawled_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) 
            DO UPDATE SET 
                stargazers_count = EXCLUDED.stargazers_count,
                description = EXCLUDED.description,
                primary_language = EXCLUDED.primary_language,
                updated_at = EXCLUDED.updated_at,
                crawled_at = EXCLUDED.crawled_at
        """

        successful = 0
        with self.connection.cursor() as cursor:
            for repo in repositories:
                try:
                    cursor.execute(
                        query,
                        (
                            repo.id,
                            repo.name,
                            repo.owner,
                            repo.name_with_owner,
                            repo.stargazers_count,
                            repo.url,
                            repo.description,
                            repo.primary_language,
                            repo.created_at,
                            repo.updated_at,
                            repo.crawled_at,
                        ),
                    )
                    successful += 1
                except Exception as e:
                    print(f"Failed to save {repo.name_with_owner}: {e}")

        self.connection.commit()
        return successful
