drop table if exists person cascade;

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
