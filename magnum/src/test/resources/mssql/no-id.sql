drop table if exists no_id;

create table no_id (
    created_at datetimeoffset not null default sysdatetimeoffset(),
    user_name varchar(200) not null,
    user_action varchar(200) not null
);

insert into no_id (created_at, user_name, user_action) values
('1997-08-15T00:00:00Z', 'Josh', 'clicked a button'),
('1997-08-16T00:00:00Z', 'Danny', 'opened a toaster'),
('1997-08-17T00:00:00Z', 'Greg', 'ran some QA tests');
