#!/usr/bin/env python3
"""
Main entry point for GitHub Repository Crawler
Follows clean architecture principles
"""

import os
import sys

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))

from src.crawler import RepositoryCrawler


def load_config():
    """Configuration loading with environment variables"""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "github_data"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "112233"),
    }


def run_data_export():
    """Run data export after crawling"""
    try:
        from export_data import main as export_main

        export_main()
    except ImportError:
        print("==>  Export script not found, skipping data export")


def main():
    """Main application entry point"""
    print("==> GitHub Repository Crawler - Optimized Version")

    # Configuration
    db_config = load_config()
    github_token = os.getenv("GITHUB_TOKEN", "")

    if not github_token:
        print("==> GitHub token not found")
        return

    # Initialize crawler
    crawler = RepositoryCrawler(github_token, db_config)

    try:
        # Start crawling - optimized for speed
        crawler.crawl(max_repositories=1000, batch_size=100)

        # Auto-export after crawling
        print("\n" + "=" * 50)
        print("==> Starting automatic data export...")
        run_data_export()

    except KeyboardInterrupt:
        print("==> Crawl interrupted by user")
    except Exception as e:
        print(f"==> Crawl failed: {e}")
    finally:
        print("==> Application finished")


if __name__ == "__main__":
    main()
