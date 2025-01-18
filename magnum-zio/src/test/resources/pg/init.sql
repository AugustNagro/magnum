create table big_dec (
    id int primary key,
    my_big_dec numeric
);

insert into big_dec values
(1, 123),
(2, null);

CREATE TABLE car (
    model VARCHAR(50) NOT NULL,
    id bigint PRIMARY KEY,
    top_speed INT NOT NULL,
    vin INT,
    color TEXT NOT NULL CHECK (color IN ('Red', 'Green', 'Blue')),
    created TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO car (model, id, top_speed, vin, color, created) VALUES
('McLaren Senna', 1, 208, 123, 'Red', '2024-11-24T22:17:30.000000000Z'::timestamptz),
('Ferrari F8 Tributo', 2, 212, 124, 'Green', '2024-11-24T22:17:31.000000000Z'::timestamptz),
('Aston Martin Superleggera', 3, 211, null, 'Blue', '2024-11-24T22:17:32.000000000Z'::timestamptz);

CREATE TABLE my_user (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name TEXT NOT NULL
);

insert into my_user (first_name) values
('George'),
('Alexander'),
('John');

create table no_id (
    created_at timestamptz not null default now(),
    user_name text not null,
    user_action text not null
);

insert into no_id values
(timestamp '1997-08-15', 'Josh', 'clicked a button'),
(timestamp '1997-08-16', 'Danny', 'opened a toaster'),
(timestamp '1997-08-17', 'Greg', 'ran some QA tests');

create table person (
    id bigint primary key,
    first_name varchar(50),
    last_name varchar(50) not null,
    is_admin boolean not null,
    created timestamptz not null,
    social_id UUID
);

insert into person (id, first_name, last_name, is_admin, created, social_id) values
(1, 'George', 'Washington', true, now(), 'd06443a6-3efb-46c4-a66a-a80a8a9a5388'),
(2, 'Alexander', 'Hamilton', true, now(), '529b6c6d-7228-4da5-81d7-13b706f78ddb'),
(3, 'John', 'Adams', true, now(), null),
(4, 'Benjamin', 'Franklin', true, now(), null),
(5, 'John', 'Jay', true, now(), null),
(6, 'Thomas', 'Jefferson', true, now(), null),
(7, 'James', 'Madison', true, now(), null),
(8, null, 'Nagro', false, timestamp '1997-08-12', null);
