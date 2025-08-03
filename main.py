import os
import requests
import psycopg2
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Use GITHUB_TOKEN from GitHub Actions or fallback to GITHUB_API from .env
TOKEN = os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_API")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
GITHUB_API = "https://api.github.com/graphql"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="github",
    user="postgres",
    password="password",
    port=5432
)
cursor = conn.cursor()

def build_query(cursor_token=None):
    return {
        "query": """
        query($cursor: String) {
          search(query: "stars:>100", type: REPOSITORY, first: 100, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                id
                name
                stargazerCount
                owner { login }
              }
            }
          }
        }
        """,
        "variables": {"cursor": cursor_token}
    }

def crawl():
    cursor_token = None
    total = 0
    while total < 100000:
        response = requests.post(GITHUB_API, json=build_query(cursor_token), headers=HEADERS)
        if response.status_code != 200:
            print("Error or rate limit:", response.text)
            time.sleep(60)
            continue

        data = response.json()["data"]["search"]
        for repo in data["nodes"]:
            try:
                cursor.execute("""
                    INSERT INTO repositories (repo_id, name, owner, stars)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (repo_id) DO NOTHING
                """, (repo["id"], repo["name"], repo["owner"]["login"], repo["stargazerCount"]))
                total += 1
            except Exception as e:
                print("Insert error:", e)

        conn.commit()
        if not data["pageInfo"]["hasNextPage"]:
            break

        cursor_token = data["pageInfo"]["endCursor"]
        print("Fetched so far:", total)

if __name__ == "__main__":
    crawl()
