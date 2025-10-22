drop table if exists mag_album;

create table mag_album (
    id bigint primary key generated always as identity,
    my_vec int[]
);
