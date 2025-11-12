#!/usr/bin/env python3
import pandas as pd
import psycopg2
import json
import csv
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_CONFIG  # Now this will work


def export_to_csv():
    """Export repository data to CSV"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        # Export to CSV
        df = pd.read_sql_query(
            """
            SELECT name_with_owner, stargazers_count, description, primary_language, 
                   created_at, updated_at, crawled_at
            FROM repositories 
            ORDER BY stargazers_count DESC
        """,
            conn,
        )

        df.to_csv("repositories_export.csv", index=False)
        print(f"Exported {len(df)} repositories to repositories_export.csv")

        # Export to JSON
        df.to_json("repositories_export.json", orient="records", indent=2)
        print(f"Exported {len(df)} repositories to repositories_export.json")

        conn.close()

    except Exception as e:
        print(f"Error exporting data: {e}")


if __name__ == "__main__":
    export_to_csv()
