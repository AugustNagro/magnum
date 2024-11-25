drop table if exists car;

CREATE TABLE car (
    model String NOT NULL,
    id Int64 NOT NULL,
    top_speed Int32 NOT NULL,
    vin Nullable(Int32),
    color Enum('Red', 'Green', 'Blue'),
    created DateTime NOT NULL
)
ENGINE = MergeTree()
ORDER BY created;

INSERT INTO car (model, id, top_speed, vin, color, created) VALUES
('McLaren Senna', 1, 208, 123, 'Red', toDateTime('2024-11-24 22:17:30', 'UTC')),
('Ferrari F8 Tributo', 2, 212, 124, 'Green', toDateTime('2024-11-24 22:17:31', 'UTC')),
('Aston Martin Superleggera', 3, 211, null, 'Blue', toDateTime('2024-11-24 22:17:32', 'UTC'));