drop table if exists car;

CREATE TABLE car (
    model String NOT NULL,
    id UUID NOT NULL,
    top_speed Int32 NOT NULL,
    created DateTime NOT NULL
)
ENGINE = MergeTree()
ORDER BY created;

INSERT INTO car (model, id, top_speed, created) VALUES
(
  'McLaren Senna',
  toUUID('a88a32f1-1e4a-41b9-9fb0-e9a8aba2428a'),
  208,
  toDateTime('2023-03-05 02:26:00')
),
(
  'Ferrari F8 Tributo',
  toUUID('e4895170-5b54-4e3b-b857-b95d45d3550c'),
  212,
  toDateTime('2023-03-05 02:27:00')
),
(
  'Aston Martin Superleggera',
  toUUID('460798da-917d-442f-a987-a7e6528ddf17'),
  211,
  toDateTime('2023-03-05 02:28:00')
);