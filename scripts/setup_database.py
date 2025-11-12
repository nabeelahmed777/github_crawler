#!/usr/bin/env python3
import psycopg2
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_CONFIG  # Now this will work


def drop_and_create_tables():
    """Drop existing tables and create new ones"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS repositories")
        print("Dropped existing repositories table")

        # Create new table
        create_table_query = """
        CREATE TABLE repositories (
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
        cursor.execute(create_table_query)

        # Create indexes
        indexes = [
            "CREATE UNIQUE INDEX idx_unique_name_with_owner ON repositories(name_with_owner)",
            "CREATE INDEX idx_stargazers_count ON repositories(stargazers_count)",
            "CREATE INDEX idx_crawled_at ON repositories(crawled_at)",
            "CREATE INDEX idx_owner ON repositories(owner)",
        ]

        for index_query in indexes:
            cursor.execute(index_query)

        conn.commit()
        print("Database setup completed successfully")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error setting up database: {e}")
        raise


def main():
    print("Setting up database...")
    drop_and_create_tables()


if __name__ == "__main__":
    main()
