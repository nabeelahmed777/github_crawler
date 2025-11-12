# GitHub Repository Crawler

A scalable GitHub repository crawler that collects star count data using GitHub's GraphQL API.

## Features

- GitHub GraphQL API integration
- PostgreSQL storage with efficient upsert operations
- Rate limit handling with retry mechanisms
- GitHub Actions automation
- Data export to CSV and JSON

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Set up PostgreSQL database
3. Add GitHub token to `.env` file
4. Run: `python main.py`

## Usage

```bash
# Setup database
python scripts/setup_database.py

# Run crawler
python main.py

# Export data
python scripts/export_data.py


# GitHub Repository Crawler
## Sofstica Assignment
Complete GitHub crawler with PostgreSQL and GraphQL API