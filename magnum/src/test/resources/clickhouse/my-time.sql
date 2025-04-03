drop table if exists my_time;

create table my_time (
  a DateTime not null,
  b Date not null,
  c String not null,
  d DateTime not null
)
engine = MergeTree()
order by a;

insert into my_time values
(toDateTime('2025-03-30 21:19:23'), toDate('2025-03-30'), '05:20:04', toDateTime('2025-04-02 20:16:38')),
(toDateTime('2025-03-31 21:19:23'), toDate('2025-03-31'), '05:30:04', toDateTime('2025-04-02T20:17:38'));
