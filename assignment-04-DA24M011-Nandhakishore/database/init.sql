CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    thumbnail BYTEA,  -- Binary storage for images
    summary TEXT
);