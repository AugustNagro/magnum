drop table if exists my_user;

create table my_user (
    first_name varchar(200) not null,
    id bigint identity(1,1) primary key
);

insert into my_user (first_name) values
('George'),
('Alexander'),
('John');
