-- Create the color_enum type
CREATE TYPE color_enum AS ENUM ('Red', 'Blue', 'Green');

-- Create the car table
CREATE TABLE IF NOT EXISTS car
(
    id        SERIAL PRIMARY KEY,
    model     TEXT,
    top_speed INT,
    vin       INT,
    color     color_enum,
    created   TIMESTAMP
);

-- Create the owner table
CREATE TABLE IF NOT EXISTS owner
(
    id     SERIAL PRIMARY KEY,
    name   TEXT,
    car_id INT REFERENCES car (id)
);