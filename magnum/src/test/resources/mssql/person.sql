drop table if exists person;

create table person (
    id bigint primary key,
    first_name varchar(50),
    last_name varchar(50) not null,
    is_admin bit not null,
    created datetimeoffset not null,
    social_id uniqueidentifier
);

insert into person (id, first_name, last_name, is_admin, created, social_id) values
(1, 'George', 'Washington', 1, sysdatetimeoffset(), 'd06443a6-3efb-46c4-a66a-a80a8a9a5388'),
(2, 'Alexander', 'Hamilton', 1, sysdatetimeoffset(), '529b6c6d-7228-4da5-81d7-13b706f78ddb'),
(3, 'John', 'Adams', 1, sysdatetimeoffset(), null),
(4, 'Benjamin', 'Franklin', 1, sysdatetimeoffset(), null),
(5, 'John', 'Jay', 1, sysdatetimeoffset(), null),
(6, 'Thomas', 'Jefferson', 1, sysdatetimeoffset(), null),
(7, 'James', 'Madison', 1, sysdatetimeoffset(), null),
(8, null, 'Nagro', 0, '1997-08-12T00:00:00Z', null);
