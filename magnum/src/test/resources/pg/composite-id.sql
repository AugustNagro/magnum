drop table if exists composite_id;

create table composite_id (
    first_id bigint not null,
    second_id bigint not null,
    created_at timestamptz not null default now(),
    user_name text not null,
    user_action text not null,
    primary key (first_id, second_id)
);

insert into composite_id values
(1, 1, timestamp '1997-08-15', 'Josh', 'clicked a button'),
(1, 2, timestamp '1997-08-16', 'Danny', 'opened a toaster'),
(2, 1, timestamp '1997-08-17', 'Greg', 'ran some QA tests');
