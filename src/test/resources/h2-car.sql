drop table if exists car;

create table car (
    model varchar(50) not null,
    id bigint auto_increment primary key,
    top_speed int,
    vin int
);

insert into car (model, top_speed, vin) values
('McLaren Senna', 208, 123),
('Ferrari F8 Tributo', 212, 124),
('Aston Martin Superleggera', 211, null);
