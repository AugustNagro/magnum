drop table if exists car;

create table car (
    model varchar(50) not null,
    id bigint primary key,
    top_speed int not null,
    vin int,
    color enum('Red', 'Green', 'Blue'),
    created datetime not null
);

insert into car (model, id, top_speed, vin, color, created) values
('McLaren Senna', 1, 208, 123, 'Red', '2024-11-24 22:17:30'),
('Ferrari F8 Tributo', 2, 212, 124, 'Green', '2024-11-24 22:17:31'),
('Aston Martin Superleggera', 3, 211, null, 'Blue', '2024-11-24 22:17:32');
