#!/usr/bin/env python3
from src.crawler import GitHubCrawler
from config import MAX_REPOSITORIES


def main():
    print("🚀 Starting GitHub Repository Crawler")
    print(f"📊 Target: {MAX_REPOSITORIES} repositories")

    crawler = GitHubCrawler()

    try:
        crawler.crawl_repositories(MAX_REPOSITORIES)
    except KeyboardInterrupt:
        print("⏹️ Crawl interrupted by user")
    except Exception as e:
        print(f"❌ Error during crawl: {e}")
    finally:
        crawler.close()

    print("✅ Crawler finished")


if __name__ == "__main__":
    main()
