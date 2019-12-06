DROP DATABASE IF EXISTS auto_reply_db;
CREATE DATABASE auto_reply_db;
\c auto_reply_db

CREATE TABLE reply (
	reply_id integer PRIMARY KEY,
	reply_text text NOT NULL,
	reply_version integer NOT NULL
);

CREATE TABLE user_reply_settings (
	user_id integer PRIMARY KEY
);

CREATE TABLE user_reply_relation (
	user_id integer NOT NULL,
	reply_id  integer NOT NULL,
	active boolean NOT NULL,
	PRIMARY KEY (user_id, reply_id),
	FOREIGN KEY (user_id) REFERENCES user_reply_settings(user_id),
	FOREIGN KEY (reply_id) REFERENCES reply(reply_id)
);