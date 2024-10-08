drop table if exists mag_car;

create table mag_car (
    id bigint primary key,
    text_colors text[] not null,
    text_color_map text[][] not null
);

insert into mag_car values
(1, '{"red_orange", "Greenish"}', '{{"red_orange", "red_orange"}, {"Greenish", "Greenish"}}'),
(2, '{"Greenish", "blue"}', '{{"red_orange", "Greenish"}, {"Greenish", "blue"}}');
