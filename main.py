import os
import requests
import psycopg2
import time

# Get GitHub token from environment
TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise Exception("GITHUB_TOKEN not found!")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
GITHUB_API = "https://api.github.com/graphql"

# PostgreSQL connection
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
    batch = []
    BATCH_SIZE = 100
    star_ranges = [(i, i + 200) for i in range(100, 200000, 200)]  # Bigger ranges = faster

    for min_star, max_star in star_ranges:
        cursor_token = None
        while True:
            query = f"stars:{min_star}..{max_star}"
            for attempt in range(5):  # Retry mechanism
                response = requests.post(
                    GITHUB_API,
                    json=build_query(cursor_token, query),
                    headers=HEADERS
                )
                if response.status_code == 200:
                    break
                else:
                    print(f"[{response.status_code}] Waiting 60s before retry...")
                    time.sleep(60)
            else:
                print("Failed after 5 attempts, skipping this range.")
                break

            search_data = response.json().get("data", {}).get("search", {})
            repos = search_data.get("nodes", [])
            for repo in repos:
                batch.append((
                    repo["id"],
                    repo["name"],
                    repo["owner"]["login"],
                    repo["stargazerCount"]
                ))
                total += 1

                if total % 100 == 0:
                    print(f"Inserted {total} repositories")

            if batch:
                try:
                    cursor.executemany("""
                        INSERT INTO repositories (repo_id, name, owner, stars)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (repo_id) DO NOTHING
                    """, batch)
                    conn.commit()
                    batch.clear()
                except Exception as e:
                    print("DB insert failed:", e)
                    conn.rollback()

            if not search_data.get("pageInfo", {}).get("hasNextPage"):
                break

            cursor_token = search_data["pageInfo"]["endCursor"]
            time.sleep(0.5)  # Reduce delay to speed up crawl

        if total >= 100000:
            break

    print(f"Done. Total repositories inserted: {total}")

if __name__ == "__main__":
    crawl()
