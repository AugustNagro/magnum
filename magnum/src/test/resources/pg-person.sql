drop table if exists person cascade;

create table person (
    id bigint primary key generated always as identity,
    first_name varchar(50),
    last_name varchar(50) not null,
    is_admin boolean not null,
    created timestamptz not null default now(),
    social_id UUID
);

insert into person (first_name, last_name, is_admin, created, social_id) values
('George', 'Washington', true, now(), 'd06443a6-3efb-46c4-a66a-a80a8a9a5388'),
('Alexander', 'Hamilton', true, now(), '529b6c6d-7228-4da5-81d7-13b706f78ddb'),
('John', 'Adams', true, now(), null),
('Benjamin', 'Franklin', true, now(), null),
('John', 'Jay', true, now(), null),
('Thomas', 'Jefferson', true, now(), null),
('James', 'Madison', true, now(), null),
(null, 'Nagro', false, timestamp '1997-08-12', null);
