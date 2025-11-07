drop table if exists composite_id;

CREATE TABLE composite_id (
    first_id Int64 NOT NULL,
    second_id Int64 NOT NULL,
    created_at DateTime NOT NULL,
    user_name String NOT NULL,
    user_action String NOT NULL
)
ENGINE = MergeTree()
ORDER BY created_at;

INSERT INTO composite_id VALUES
(1, 1, timestamp '1997-08-15', 'Josh', 'clicked a button'),
(1, 2, timestamp '1997-08-16', 'Danny', 'opened a toaster'),
(2, 1, timestamp '1997-08-17', 'Greg', 'ran some QA tests');