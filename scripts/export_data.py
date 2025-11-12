"""
Data export utility for GitHub Repository Crawler
Exports data to CSV and JSON formats
"""

import psycopg2
import csv
import json
import os
from datetime import datetime


def get_database_connection():
    """Create database connection using environment variables"""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "github_data"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "112233"),
    }
    return psycopg2.connect(**db_config)


def export_to_csv_and_json():
    """Export repository data to both CSV and JSON formats"""
    try:
        print("===>  Starting data export...")

        # Database connection
        conn = get_database_connection()
        cursor = conn.cursor()

        # Get all repository data
        cursor.execute("""
            SELECT 
                name_with_owner,
                stargazers_count,
                description,
                primary_language,
                created_at,
                updated_at,
                crawled_at
            FROM repositories 
            ORDER BY stargazers_count DESC
        """)

        # Fetch data
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        print(f" ==? Found {len(rows)} repositories in database")

        # Export to CSV
        csv_filename = "repositories_export.csv"
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            writer.writerow(columns)

            # Write data rows
            writer.writerows(rows)

        print(f" ===> CSV export completed: {csv_filename}")

        # Export to JSON
        json_filename = "repositories_export.json"
        data = []

        for row in rows:
            # Convert row to dictionary
            row_dict = dict(zip(columns, row))

            # Convert datetime objects to strings for JSON
            for key, value in row_dict.items():
                if hasattr(value, "isoformat"):
                    row_dict[key] = value.isoformat()

            data.append(row_dict)

        with open(json_filename, "w", encoding="utf-8") as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)

        print(f"=== JSON export completed: {json_filename}")

        # Generate summary
        generate_export_summary(data, len(rows))

        cursor.close()
        conn.close()

        print(" === Data export completed successfully!")

    except Exception as e:
        print(f"=== Error during data export: {e}")
        raise


def generate_export_summary(data, total_count):
    """Generate and display export summary"""
    print("\n === EXPORT SUMMARY: ===")
    print(f"   Total repositories: {total_count}")

    if data:
        # Top 5 most starred repos
        top_repos = sorted(data, key=lambda x: x["stargazers_count"], reverse=True)[:5]
        print(
            f"   Top repository: {top_repos[0]['name_with_owner']} ({top_repos[0]['stargazers_count']} stars)"
        )

        # Language distribution
        languages = {}
        for repo in data:
            lang = repo["primary_language"] or "Unknown"
            languages[lang] = languages.get(lang, 0) + 1

        top_language = max(languages.items(), key=lambda x: x[1])
        print(f"   Most common language: {top_language[0]} ({top_language[1]} repos)")

    print(f"   Generated files: repositories_export.csv, repositories_export.json")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main function for data export"""
    print("==>  GitHub Repository Data Export Tool")
    print("=" * 40)

    try:
        export_to_csv_and_json()
    except KeyboardInterrupt:
        print("\n⏹ ==>  Export cancelled by user")
    except Exception as e:
        print(f"\n ==> Export failed: {e}")


if __name__ == "__main__":
    main()
