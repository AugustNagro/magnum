drop table if exists person cascade;

create table person (
    id bigint auto_increment primary key,
    first_name varchar(50),
    last_name varchar(50) not null,
    is_admin boolean not null,
    created timestamp default current_timestamp
);

insert into person (first_name, last_name, is_admin, created) values
('George', 'Washington', true, now()),
('Alexander', 'Hamilton', true, now()),
('John', 'Adams', true, now()),
('Benjamin', 'Franklin', true, now()),
('John', 'Jay', true, now()),
('Thomas', 'Jefferson', true, now()),
('James', 'Madison', true, now()),
(null, 'Nagro', false, timestamp '1997-08-12');
