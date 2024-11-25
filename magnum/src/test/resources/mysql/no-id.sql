drop table if exists no_id;

create table no_id (
    created_at datetime not null default now(),
    user_name varchar(200) not null,
    user_action varchar(200) not null
);

insert into no_id values
('1997-08-15', 'Josh', 'clicked a button'),
('1997-08-16', 'Danny', 'opened a toaster'),
('1997-08-17', 'Greg', 'ran some QA tests');
