drop table if exists mag_user;
drop type if exists color;

create type Color as enum ('Red', 'Green', 'Blue');

create table mag_user (
    id bigint primary key,
    name text not null,
    friends text[] not null,
    matrix integer[][] not null,
    test integer[] not null,
    dates timestamptz[] not null,
    bx box not null,
    c circle not null,
    iv interval not null,
    l line not null,
    lSeg lseg not null,
    p path not null,
    pnt point not null,
    poly polygon not null,
    colors Color[] not null,
    colorMap Color[][] not null
);

insert into mag_user values
(1, 'Abby', '{"Jane", "Mary"}', '{{1, 2}, {3, 4}, {5, 6}}', '{1}', '{"2023-07-30T12:21:36Z", "2023-07-30T12:21:37Z"}', '(1, 2, 3, 4)', '<(1, 2), 3>', '1 hour', '{1, 1, 1}', '1, 1, 2, 2', '[(1, 1), (2, 2)]', '(1, 1)', '((0, 0), (-1, 1), (1, 1))', '{"Red", "Green"}', '{{"Red", "Red"}, {"Green", "Green"}}'),
(2, 'Jacob', '{"Grace", "Aubrey"}', '{{7, 8}, {9, 10}}', '{}', '{}', '(5, 6, 7, 8)', '<(4, 5), 6>', '2 days', '{2, 2, 2}', '2, 2, 3, 3', '[(2, 2), (3, 3)]', '(2, 2)', '((0, 0), (-1, -1), (1, -1))', '{"Green", "Blue"}', '{{"Red", "Green"}, {"Green", "Blue"}}');
