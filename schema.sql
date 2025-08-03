CREATE TABLE IF NOT EXISTS repositories (
    id SERIAL PRIMARY KEY,
    repo_id TEXT UNIQUE,
    name TEXT,
    owner TEXT,
    stars INT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
