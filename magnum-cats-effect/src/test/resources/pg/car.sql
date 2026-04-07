DROP TABLE IF EXISTS car;
DROP TYPE IF EXISTS Color;

CREATE TYPE Color as enum ('Red', 'Green', 'Blue');

CREATE TABLE car (
    model VARCHAR(50) NOT NULL,
    id bigint PRIMARY KEY,
    top_speed INT NOT NULL,
    vin INT,
    color Color NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO car (model, id, top_speed, vin, color, created) VALUES
('McLaren Senna', 1, 208, 123, 'Red', '2024-11-24T22:17:30.000000000Z'::timestamptz),
('Ferrari F8 Tributo', 2, 212, 124, 'Green', '2024-11-24T22:17:31.000000000Z'::timestamptz),
('Aston Martin Superleggera', 3, 211, null, 'Blue', '2024-11-24T22:17:32.000000000Z'::timestamptz);
