-- Create the database
CREATE DATABASE news_db;

-- Connect to the database
\c news_db; -- \c is only opplicable for SQL not a general command 

-- Create tables
CREATE TABLE news_articles (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    article_url TEXT NOT NULL,
    publication_date TEXT NOT NULL
);

CREATE TABLE news_images (
    id SERIAL PRIMARY KEY,
    article_id INTEGER REFERENCES news_articles(id),
    thumbnail_url TEXT NOT NULL,
    image_data BYTEA NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX idx_publication_date 
ON news_articles(publication_date);

CREATE INDEX idx_article_id 
ON news_images(article_id);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE news_db TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;