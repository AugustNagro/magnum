drop table if exists person;

create table person (
    id UUID not null default generateUUIDv4(),
    first_name Nullable(String),
    last_name String not null,
    is_admin Bool not null,
    created DateTime not null default now()
)
engine = MergeTree()
order by created;

insert into person values
(
    toUUID('3b1bc33b-ecc9-45c7-b866-04c60d31d687'),
    'George',
    'Washington',
    true,
    toDateTime('2023-03-05 02:26:00')
),
(
    toUUID('12970806-606d-42ff-bb9c-3187bbd360dd'),
    'Alexander',
    'Hamilton',
    true,
    toDateTime('2023-03-05 02:27:00')
),
(
    toUUID('834a2bd2-6842-424f-97e0-fe5ed02c3653'),
    'John',
    'Adams',
    true,
    toDateTime('2023-03-05 02:28:00')
),
(
    toUUID('60492bb2-fe02-4d02-9c6d-ae03fa6f2243'),
    'Benjamin',
    'Franklin',
    true,
    toDateTime('2023-03-05 02:29:00')
),
(
    toUUID('2244eef4-b581-4305-824f-efe8f70e6bb7'),
    'John',
    'Jay',
    true,
    toDateTime('2023-03-05 02:30:00')
),
(
    toUUID('fb3c479a-0521-4f06-b6ad-218db867518c'),
    'Thomas',
    'Jefferson',
    true,
    toDateTime('2023-03-05 02:31:00')
),
(
    toUUID('8fefe1d8-20eb-44a0-84da-d328334e1e11'),
    'James',
    'Madison',
    true,
    toDateTime('2023-03-05 02:32:00')
),
(
    toUUID('8a2d842a-0d03-463c-8c34-43b38120f9e4'),
    null,
    'Nagro',
    false,
    toDateTime('2023-03-05 02:33:00')
);