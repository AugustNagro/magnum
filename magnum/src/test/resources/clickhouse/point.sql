DROP TABLE IF EXISTS point;

CREATE TABLE point
(
    x Int32 NOT NULL,
    y Int32 NOT NULL,
    descr String NOT NULL,
    PRIMARY KEY (x, y)
)
ENGINE = MergeTree()
ORDER BY (x, y);

INSERT INTO point VALUES
(1, 1, 'hello'),
(1, 2, 'world');
