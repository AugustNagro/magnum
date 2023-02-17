drop table if exists car;

create table car (
    id bigint primary key generated always as identity,
    model varchar(50) not null,
    top_speed int
);

insert into car (model, top_speed) values
('McLaren Senna', 208),
('Ferrari F8 Tributo', 212),
('Aston Martin Superleggera', 211);
