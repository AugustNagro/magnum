DROP TABLE IF EXISTS comp_id;

CREATE TABLE comp_id
(
    a String,
    b Int32,
    c Int32,
    d String,
    PRIMARY KEY (b, d)
)
ENGINE = MergeTree()
ORDER BY (b, d);

INSERT INTO comp_id VALUES
('alpha', 1, 10, 'first'),
('beta', 2, 20, 'second'),
('gamma', 3, 30, 'third');
