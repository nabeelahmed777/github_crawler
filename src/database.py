import psycopg2
from psycopg2.extras import execute_batch
from config import DB_CONFIG


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**DB_CONFIG)
            print("Connected to database successfully")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def create_tables(self):
        """Create the repositories table"""
        create_table_query = """
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
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
            self.connection.commit()
            print("Tables created successfully")
        except Exception as e:
            print(f"Error creating tables: {e}")
            self.connection.rollback()
            raise

        # Create indexes separately
        self.create_indexes()

    def create_indexes(self):
        """Create indexes for better performance"""
        indexes = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_name_with_owner ON repositories(name_with_owner)",
            "CREATE INDEX IF NOT EXISTS idx_stargazers_count ON repositories(stargazers_count)",
            "CREATE INDEX IF NOT EXISTS idx_crawled_at ON repositories(crawled_at)",
            "CREATE INDEX IF NOT EXISTS idx_owner ON repositories(owner)",
        ]

        try:
            with self.connection.cursor() as cursor:
                for index_query in indexes:
                    cursor.execute(index_query)
            self.connection.commit()
            print("Indexes created successfully")
        except Exception as e:
            print(f"Error creating indexes: {e}")
            self.connection.rollback()

    def upsert_repository(self, repo):
        """Insert or update repository data"""
        query = """
        INSERT INTO repositories 
            (id, name, owner, name_with_owner, stargazers_count, url, description, primary_language, created_at, updated_at, crawled_at, last_updated)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id) 
        DO UPDATE SET 
            stargazers_count = EXCLUDED.stargazers_count,
            description = EXCLUDED.description,
            primary_language = EXCLUDED.primary_language,
            updated_at = EXCLUDED.updated_at,
            crawled_at = EXCLUDED.crawled_at,
            last_updated = CURRENT_TIMESTAMP
        """

        try:
            with self.connection.cursor() as cursor:
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
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error upserting repository {repo.name_with_owner}: {e}")
            self.connection.rollback()
            return False

    def get_repository_count(self):
        """Get total number of repositories in database"""
        query = "SELECT COUNT(*) FROM repositories"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error getting repository count: {e}")
            return 0

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
