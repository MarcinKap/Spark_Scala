create table weather_data
(
	id int,
	weather text
);

create table timestamp
(
	timestamp timestamp,
	day int,
	month int,
	year int,
	hour int
);

create table authority
(
	local_authority_ons_code text,
	local_authority_name text,
	region_ons_code text,
	region_name text
);

create table road_type
(
	id int,
	road_type text,
	road_category text
);

create table vehicle_type
(
	id int,
	vehicle_type text,
	vehicle_category text,
	has_engine bool
);


create table facts
(
	weather_data_id int,
	timestamp timestamp,
	local_authority_ons_code text,
	road_type_id int,
	vehicle_type_id int,
	vehicles_count int

);