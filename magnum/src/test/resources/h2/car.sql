drop table if exists car;

create table car (
    model varchar(50) not null,
    id bigint auto_increment primary key,
    top_speed int not null,
    vin int,
    color enum('Red', 'Green', 'Blue'),
    created timestamp with time zone not null
);

insert into car (model, top_speed, vin, color, created) values
('McLaren Senna', 208, 123, 'Red', '2024-11-24T22:17:30.000000000Z'),
('Ferrari F8 Tributo', 212, 124, 'Green', '2024-11-24T22:17:31.000000000Z'),
('Aston Martin Superleggera', 211, null, 'Blue', '2024-11-24T22:17:32.000000000Z');
