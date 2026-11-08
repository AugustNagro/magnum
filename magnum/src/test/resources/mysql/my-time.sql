drop table if exists my_time cascade;

create table my_time (
  a timestamp not null,
  b date not null,
  c time not null,
  d datetime not null
);

insert into my_time values
('2025-03-30 21:19:23', '2025-03-30', '05:20:04', '2025-04-02 20:16:38'),
('2025-03-31 21:19:23', '2025-03-31', '05:30:04', '2025-04-02T20:17:38');