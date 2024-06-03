create view forestation as
select fa.country_code,
       fa.country_name,
       fa.year,
       fa.forest_area_sqkm,
       la.total_area_sq_mi * 2.59                                 as total_area_sqkm,
       (fa.forest_area_sqkm / (la.total_area_sq_mi * 2.59)) * 100 as forest_percent,
       r.region,
       r.income_group
from forest_area as fa
         join land_area as la on la.country_code = fa.country_code and la.year = fa.year
         join regions as r on fa.country_code = r.country_code
order by country_code;


select forest_area_sqkm as total_forest_area
from forestation
where year = 1990 and country_name = 'World';


select forest_area_sqkm as total_forest_area
from forestation
where year = 2016 and country_name = 'world';


select cur.forest_area_sqkm - pre.forest_area_sqkm as dif_area
from forest_area as cur
         join forest_area as pre
              on (
                  cur.year = '2016'
                      and pre.year = '1990'
                      and cur.country_name = 'World'
                      and pre.country_name = 'World'
                  );


select ((pre.forest_area_sqkm - cur.forest_area_sqkm) / pre.forest_area_sqkm) * 100 as dif_percent
from forest_area as cur
         join forest_area as pre
              on (
                  cur.year = '2016'
                      and pre.year = '1990'
                      and cur.country_name = 'World'
                      and pre.country_name = 'World'
                  );



select country_name,
       total_area_sqkm,
       ABS((total_area_sqkm) - (select pre.forest_area_sqkm - cur.forest_area_sqkm as dif_area
                                from forest_area as cur
                                         join forest_area as pre
                                              on (
                                                  cur.year = '2016'
                                                      and pre.year = '1990'
                                                      and cur.country_name = 'World'
                                                      and pre.country_name = 'World'
                                                  ))) as dif
from forestation
where year = '2016'
order by dif limit 1


create view region_forest as
select region.*, (region.total_forest / region.total_area) * 100 as percent_forest
from (select region, year, sum (forest_area_sqkm) as total_forest, sum (total_area_sqkm) as total_area
      from forestation
      group by region, year
      having (year = 2016 or year = 1990)) as region
order by region,
year

select round(cast(percent_forest as numeric), 2) as percent_forest_world
from region_forest
where year = 2016 and region = 'World';


select region,
       round(cast(total_forest as numeric), 2)   as total_area,
       round(cast(percent_forest as numeric), 2) as percent_forest
from region_forest
where year = 2016
  and round(cast (percent_forest as numeric)
    , 2)
    = (select max (round(cast (percent_forest as numeric)
    , 2)) as max_percent
    from region_forest
    where year = 2016)


select a.region,
       round(cast(a.percent_forest as numeric), 2) as percent_forest_1990,
       round(cast(b.percent_forest as numeric), 2) as percent_forest_2016
from (select * from region_forest where year = 1990) as a
         join (select * from region_forest where year = 2016) b on a.region = b.region
where a.percent_forest > b.percent_forest with _1990 as
(select * from forest_area
 where year = 1990 and forest_area_sqkm is not null and country_name != 'world'
),
_2016 as (select * from forest_area f
where year = 2016 and forest_area_sqkm is not null and country_name != 'world'
)


select _1990.country_code,
       _1990.country_name,
       r.region,
       _1990.forest_area_sqkm                          as forest_1990,
       _2016.forest_area_sqkm                          as forest_2016,
       _1990.forest_area_sqkm - _2016.forest_area_sqkm as change_forest
from (select * from forest_area where year = 1990) as _1990
         join (select * from forest_area where year = 2016) as _2016
              on _1990.country_code = _2016.country_code
         join regions as r on _2016.country_code = r.country_code
where _1990.forest_area_sqkm is not null
  and _2016.forest_area_sqkm is not null
  and r.country_name != 'world'
order by change_forest desc limit 5


with _1990 as
    (select * from forest_area
    where year = 1990 and forest_area_sqkm is not null and country_name != 'world'
    ), _2016 as (select * from forest_area f
    where year = 2016 and forest_area_sqkm is not null and country_name != 'world'
    )


select _1990.country_code,
       _1990.country_name,
       r.region,
       _1990.forest_area_sqkm                                                  as forest_1990,
       _2016.forest_area_sqkm                                                  as forest_2016,
       _1990.forest_area_sqkm - _2016.forest_area_sqkm                         as change_forest,
       round(
               cast(
                       (
                           (_2016.forest_area_sqkm - _1990.forest_area_sqkm)
                               / _1990.forest_area_sqkm * 100) as numeric), 2) as change_percent
from (select * from forest_area where year = 1990) as _1990
         join (select * from forest_area where year = 2016) as _2016
              on _1990.country_code = _2016.country_code
         join regions as r on _2016.country_code = r.country_code
where _1990.forest_area_sqkm is not null
  and _2016.forest_area_sqkm is not null
  and r.country_name != 'World'
order by change_percent limit 5


with temp as (
    select country_name, case
    when forest_percent < 25 then 'Q1'
    when forest_percent >= 25 and forest_percent < 50 then 'Q2'
    when forest_percent >= 50 and forest_percent < 75 then 'Q3'
    else 'Q4' end as quartiles
    from forestation
    where year = 2016 and forest_percent is not null
    )
select distinct quartiles, (count(country_name) over (partition by quartiles)) as countries
from temp
order by quartiles


select distinct country_name, region, forest_percent
from forestation
where forest_percent > 75 and year = 2016

SELECT count(country_name)
from forestation
where year = 2016
  and forest_percent > (
                            select forest_percent
                            from forestation
                            where country_name = 'United States' and year =2016
                        )
