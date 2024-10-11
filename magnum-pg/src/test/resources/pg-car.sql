drop table if exists mag_car;

create table mag_car (
    id bigint primary key,
    text_colors text[] not null,
    text_color_map text[][] not null,
    last_service json,
    my_json_b jsonb
);

insert into mag_car values
(1, '{"red_orange", "Greenish"}', '{{"red_orange", "red_orange"}, {"Greenish", "Greenish"}}', '{"mechanic": "Bob", "date": "2024-05-04"}', '{"a": [1, 2, 3], "b": "hello world"}'),
(2, '{"Greenish", "blue"}', '{{"red_orange", "Greenish"}, {"Greenish", "blue"}}', null, null);
