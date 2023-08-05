drop table if exists mag_user;

create table mag_user (
    id bigint primary key,
    name text not null,
    friends text[] not null,
    matrix integer[][] not null,
    test integer[] not null,
    dates timestamptz[] not null,
    bx box not null,
    c circle not null,
    iv interval not null
);

insert into mag_user values
(1, 'Abby', '{"Jane", "Mary"}', '{{1, 2}, {3, 4}, {5, 6}}', '{1}', '{"2023-07-30T12:21:36Z", "2023-07-30T12:21:37Z"}', '(1, 2, 3, 4)', '<(1, 2), 3>', '1 hour'),
(2, 'Jacob', '{"Grace", "Aubrey"}', '{{7, 8}, {9, 10}}', '{}', '{}', '(5, 6, 7, 8)', '<(4, 5), 6>', '2 days');
