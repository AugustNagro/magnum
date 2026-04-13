drop table if exists composite_id;

create table composite_id (
    first_id bigint not null,
    second_id bigint not null,
    created_at datetime not null default now(),
    user_name varchar(200) not null,
    user_action varchar(200) not null,
    primary key (first_id, second_id)
);

insert into composite_id values
(1, 1, '1997-08-15', 'Josh', 'clicked a button'),
(1, 2, '1997-08-16', 'Danny', 'opened a toaster'),
(2, 1, '1997-08-17', 'Greg', 'ran some QA tests');
