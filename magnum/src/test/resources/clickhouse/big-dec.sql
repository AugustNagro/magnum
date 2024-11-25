drop table if exists big_dec;

create table big_dec (
    id Int64 NOT NULL,
    my_big_dec Nullable(Int256)
)
ENGINE = MergeTree()
ORDER BY id;

insert into big_dec values
(1, 123),
(2, null);