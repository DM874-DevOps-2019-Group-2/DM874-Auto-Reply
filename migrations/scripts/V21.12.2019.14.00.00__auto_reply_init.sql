CREATE TABLE auto_reply (
    user_id integer PRIMARY KEY,
    reply_text text NOT NULL DEFAULT(''),
    enabled boolean NOT NULL DEFAULT(false)
);