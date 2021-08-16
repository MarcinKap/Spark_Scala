%sql
select d_roads.road_category, sum(f_facts.vehicle_count), d_time.year
from f_facts 
    join d_vehicle on f_facts.vehicle_id = d_vehicle.id
    join d_time on f_facts.timestamp = d_time.timestamp
    join d_roads on f_facts.road_category = d_roads.id
where d_vehicle.id = 2 
group by d_time.year, d_roads.road_category


%sql
select sum(f_facts.vehicle_count),
    d_time.hour,
    case
        when d_vehicle.vehicle_type in ('hgvs_2_rigid_axle', 'hgvs_3_rigid_axle', 'hgvs_3_or_4_articulated_axle') then 'less_than_4'
        else 'more_than_4'
    end axes
from f_facts 
    join d_vehicle on f_facts.vehicle_id = d_vehicle.id
    join d_time on f_facts.timestamp = d_time.timestamp
where d_vehicle.vehicle_category = "hgvs" 
group by d_time.hour, axes


%sql
select d_vehicle.vehicle_type,
    sum(f_facts.vehicle_count),
    case
        when d_time.month in (12,1,2) then 'winter'
        when d_time.month in (3,4,5) then 'spring'
        when d_time.month in (6,7,8) then 'summer'
        else 'autumn'
    end season
from f_facts 
    join d_vehicle on f_facts.vehicle_id = d_vehicle.id
    join d_time on f_facts.timestamp = d_time.timestamp
where d_vehicle.id in (1,2) 
group by season, d_vehicle.vehicle_type