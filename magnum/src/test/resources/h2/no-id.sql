drop table if exists no_id;

create table no_id (
    created_at timestamp with time zone default now() not null,
    user_name varchar not null,
    user_action varchar not null
);

insert into no_id values
(timestamp '1997-08-15', 'Josh', 'clicked a button'),
(timestamp '1997-08-16', 'Danny', 'opened a toaster'),
(timestamp '1997-08-17', 'Greg', 'ran some QA tests');
