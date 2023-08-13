drop table if exists car;

create table car (
    model varchar(50) not null,
    id bigint auto_increment primary key,
    top_speed int,
    vin int,
    color enum('Red', 'Green', 'Blue')
);

insert into car (model, top_speed, vin, color) values
('McLaren Senna', 208, 123, 'Red'),
('Ferrari F8 Tributo', 212, 124, 'Green'),
('Aston Martin Superleggera', 211, null, 'Blue');
