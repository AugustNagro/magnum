drop table if exists person;

create table person (
    id Int64 not null,
    first_name Nullable(String),
    last_name String not null,
    is_admin Bool not null,
    created DateTime not null,
    social_id Nullable(UUID)
)
engine = MergeTree()
order by created;

insert into person values
(1, 'George', 'Washington', true, toDateTime('2023-03-05 02:26:00'), toUUID('d06443a6-3efb-46c4-a66a-a80a8a9a5388')),
(2, 'Alexander', 'Hamilton', true, toDateTime('2023-03-05 02:27:00'), toUUID('529b6c6d-7228-4da5-81d7-13b706f78ddb')),
(3, 'John', 'Adams', true, toDateTime('2023-03-05 02:28:00'), null),
(4, 'Benjamin', 'Franklin', true, toDateTime('2023-03-05 02:29:00'), null),
(5, 'John', 'Jay', true, toDateTime('2023-03-05 02:30:00'), null),
(6, 'Thomas', 'Jefferson', true, toDateTime('2023-03-05 02:31:00'), null),
(7, 'James', 'Madison', true, toDateTime('2023-03-05 02:32:00'), null),
(8, null, 'Nagro', false, toDateTime('2023-03-05 02:33:00'), null);