drop table if exists car;
drop type if exists color;

create type Color as enum ('Red', 'Green', 'Blue');

create table car (
    model varchar(50) not null,
    id bigint primary key generated always as identity,
    top_speed int not null,
    vin int,
    color Color not null
);

insert into car (model, top_speed, vin, color) values
('McLaren Senna', 208, 123, 'Red'),
('Ferrari F8 Tributo', 212, 124, 'Green'),
('Aston Martin Superleggera', 211, null, 'Blue');
