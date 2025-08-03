import os
import requests
import psycopg2
import time

# Get token from GitHub Actions securely
TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise Exception("GITHUB_TOKEN not found in environment!")

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

def build_query(cursor_token=None, query_str="stars:>100"):
    return {
        "query": """
        query($cursor: String, $query: String!) {
          search(query: $query, type: REPOSITORY, first: 100, after: $cursor) {
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
        "variables": {"cursor": cursor_token, "query": query_str}
    }

def crawl():
    total = 0
    star_ranges = [(i, i + 100) for i in range(100, 100000, 100)]
    seen_repo_ids = set()

    for min_star, max_star in star_ranges:
        cursor_token = None
        while True:
            query = f"stars:{min_star}..{max_star}"
            response = requests.post(
                GITHUB_API,
                json=build_query(cursor_token, query),
                headers=HEADERS
            )

            if response.status_code != 200:
                print(f"[{response.status_code}] Waiting 60s...")
                time.sleep(60)
                continue

            data = response.json()["data"]["search"]

            for repo in data["nodes"]:
                repo_id = repo["id"]
                if repo_id in seen_repo_ids:
                    continue

                seen_repo_ids.add(repo_id)

                cursor.execute("""
                    INSERT INTO repositories (repo_id, name, owner, stars)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (repo_id) DO NOTHING
                """, (repo_id, repo["name"], repo["owner"]["login"], repo["stargazerCount"]))
                total += 1

                if total % 100 == 0:
                    print(f"Inserted {total} repositories")

            conn.commit()

            if not data["pageInfo"]["hasNextPage"]:
                break

            cursor_token = data["pageInfo"]["endCursor"]
            time.sleep(1)

        if total >= 100000:
            break

    print(f"Done. Total repositories inserted: {total}")

if __name__ == "__main__":
    crawl()
