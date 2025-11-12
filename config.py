import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration - CHANGE DATABASE NAME to match your .env file
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "github_data"),  # Changed to github_data
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "112233"),  # Changed to match your password
}

# GitHub API configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "ghp_default_token")
GRAPHQL_URL = "https://api.github.com/graphql"

# Crawler configuration
BATCH_SIZE = 50  # Reduced for testing
MAX_REPOSITORIES = 1000  # Reduced for testing
RATE_LIMIT_DELAY = 1
