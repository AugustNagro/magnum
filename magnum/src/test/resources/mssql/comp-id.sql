drop table if exists comp_id;

create table comp_id (
    a varchar(50),
    b int not null,
    c int,
    d varchar(50) not null,
    primary key (b, d)
);

insert into comp_id (a, b, c, d) values
('alpha', 1, 10, 'first'),
('beta', 2, 20, 'second'),
('gamma', 3, 30, 'third');
