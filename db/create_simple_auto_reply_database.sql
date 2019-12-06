DROP DATABASE IF EXISTS auto_reply_db;
CREATE DATABASE auto_reply_db;

\c auto_reply_db

CREATE TABLE auto_reply (
	user_id integer PRIMARY KEY,
	reply_text text NOT NULL
);