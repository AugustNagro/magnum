drop table if exists my_time;

create table my_time (
  a datetimeoffset not null,
  b date not null,
  c time not null,
  d datetime2 not null
);

insert into my_time (a, b, c, d) values
('2025-03-30T21:19:23Z', '2025-03-30', '05:20:04', '2025-04-02T20:16:38'),
('2025-03-31T21:19:23Z', '2025-03-31', '05:30:04', '2025-04-02T20:17:38');
