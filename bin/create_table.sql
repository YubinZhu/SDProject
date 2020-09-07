drop table if exists sd_entity_data;
create table sd_entity_data (
   id serial not null,
   label text not null,
   create_time timestamp not null,
   entity_name text not null,
   province text,
   city text,
   lon decimal(5,2),
   lat decimal(5,2),
   category text,
   geom geometry('multipolygon', 4326, 2),
   data jsonb,
   is_valid boolean not null,
   extra jsonb,
   primary key (id, label)
) partition by list (label);
create table sd_entity_data_test partition of sd_entity_data for values in ('test');
create index on sd_entity_data_test (label);
insert into sd_entity_data (label, create_time, entity_name, province, city, lon, lat, category, geom, data, is_valid, extra) values ('test', 'now', 'sd_company', 'beijing', 'beijing', 0.123, 0.456, 'company', st_geomfromtext('multipolygon(((12.34 56.78, 65.43 21.09, 10.01 20.02, 12.34 56.78)))', 4326), '{"a": ["b"]}', true, null);
select * from sd_entity_data;
select label, count(1) as n from sd_entity_data group by label;
select jsonb_array_element_text(data->'a', 0) as data from sd_entity_data;


select * from sd_entity_data where label = 'listed_company' and data->>'CCID_industry' = '高端装备'