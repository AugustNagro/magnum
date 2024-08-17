drop table if exists mag_car;

create table mag_car (
    id bigint primary key,
    text_colors text[] not null,
    text_color_map text[][] not null
);

insert into mag_car values
(1, '{"Red", "Green"}', '{{"Red", "Red"}, {"Green", "Green"}}'),
(2, '{"Green", "Blue"}', '{{"Red", "Green"}, {"Green", "Blue"}}');
