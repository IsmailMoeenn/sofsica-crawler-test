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
    seen_repo_ids = set()

    while total < 100000:
        try:
            response = requests.post(
                GITHUB_API,
                json=build_query(cursor_token),
                headers=HEADERS
            )

            if response.status_code != 200:
                print(f"[{response.status_code}] Retrying after 60 seconds...")
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
                print("No more pages available.")
                break

            cursor_token = data["pageInfo"]["endCursor"]
            time.sleep(1)

        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(30)

    print(f"Completed crawling. Total repositories inserted: {total}")

if __name__ == "__main__":
    crawl()
