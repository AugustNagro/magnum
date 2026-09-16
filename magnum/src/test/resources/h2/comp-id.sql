drop table if exists comp_id cascade;

create table comp_id (
    a varchar(50),
    b int,
    c int,
    d varchar(50),
    primary key (b, d)
);

insert into comp_id values
('alpha', 1, 10, 'first'),
('beta', 2, 20, 'second'),
('gamma', 3, 30, 'third');
