drop table if exists point cascade;

create table point (
    x int not null,
    y int not null,
    descr varchar(50) not null,
    primary key (x, y)
);

insert into point values
(1, 1, 'hello'),
(1, 2, 'world');