drop table if exists pg_optional_arrays;

create table pg_optional_arrays (
    id uuid primary key,
    vector_values bigint[],
    i_array_values integer[],
    array_values integer[]
);
