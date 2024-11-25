drop table if exists my_user cascade;

create table my_user (
    first_name varchar(200) not null,
    id bigint auto_increment primary key
);

insert into my_user (first_name) values
('George'),
('Alexander'),
('John');
