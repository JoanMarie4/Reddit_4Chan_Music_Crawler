-- Add up migration script here
ALTER TABLE posts ADD COLUMN toxicity JSONB;