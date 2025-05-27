drop table if exists i_array_album;

create table i_array_album (
    id bigint primary key generated always as identity,
    my_iarray int[]
);
