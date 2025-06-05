-- Add up migration script here
CREATE INDEX ON posts (board, thread_number, post_number, artists);
CREATE INDEX ON posts (board, artists);