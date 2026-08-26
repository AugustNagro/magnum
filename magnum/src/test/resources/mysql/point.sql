DROP TABLE IF EXISTS point;

CREATE TABLE point (
    x INT NOT NULL,
    y INT NOT NULL,
    descr VARCHAR(50) NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO point VALUES
(1, 1, 'hello'),
(1, 2, 'world');
