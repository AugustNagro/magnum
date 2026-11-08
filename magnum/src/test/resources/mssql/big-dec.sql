drop table if exists big_dec;

create table big_dec (
    id int primary key,
    my_big_dec numeric(38, 0)
);

insert into big_dec (id, my_big_dec) values
(1, 123),
(2, null);
