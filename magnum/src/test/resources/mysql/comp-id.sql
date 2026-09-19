DROP TABLE IF EXISTS comp_id;

CREATE TABLE comp_id (
    a VARCHAR(50),
    b INT,
    c INT,
    d VARCHAR(50),
    PRIMARY KEY (b, d)
);

INSERT INTO comp_id VALUES
('alpha', 1, 10, 'first'),
('beta', 2, 20, 'second'),
('gamma', 3, 30, 'third');
