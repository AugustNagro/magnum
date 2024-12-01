drop table if exists no_id;

CREATE TABLE no_id (
    created_at DateTime NOT NULL,
    user_name String NOT NULL,
    user_action String NOT NULL
)
ENGINE = MergeTree()
ORDER BY created_at;

INSERT INTO no_id VALUES
(timestamp '1997-08-15', 'Josh', 'clicked a button'),
(timestamp '1997-08-16', 'Danny', 'opened a toaster'),
(timestamp '1997-08-17', 'Greg', 'ran some QA tests');