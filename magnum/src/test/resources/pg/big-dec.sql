drop table if exists big_dec cascade;

create table big_dec (
    id int primary key,
    my_big_dec numeric
);

insert into big_dec values
(1, 123),
(2, null);