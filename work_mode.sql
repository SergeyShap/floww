with table_1 as (
select
	tab_num, start_date,
	case
		when finish_date = '9999-12-31'
		and end_da notnull then end_da
		else finish_date
	end as finish_date, wday_type01 mo,
	wday_type02 tu, wday_type03 we,
	wday_type04 th, wday_type05 fr,
	wplace_type
from
	workmode),
table_2 as (
select
	date(dt) dt
from
	generate_series(
	'2020-09-01'::date,
	'2020-12-31'::date, '1 day') dt),
table_3 as (
select *
	from table_1
	cross join table_2),
table_4 as (
select *,
	(case
	when dt < '2020-09-01'::date then null
	when dt >= '2020-09-01'::date and dt <= finish_date
		then extract(week from dt::date) + 1 - extract(week from '2020-09-01'::date)
	else null
	end) as week,
	extract(dow from dt::timestamp) as day_of_week
from table_3),
prefinal as (select
	tab_num,
	dt,
	(case
		when day_of_week in (0, 6) or week isnull then null
		when day_of_week = 1 and mo in (0, 1)
			and wplace_type in (0, 1, 2)
			and dt between start_date
			and finish_date
		then (1 - mo::int)
		when day_of_week = 2
			and tu in (0, 1)
			and wplace_type in (0, 1, 2)
			and dt between start_date and finish_date
		then (1 - tu::int)
		when day_of_week = 3
			and we in (0, 1)
			and wplace_type in (0, 1, 2)
			and dt between start_date and finish_date
		then (1 - we::int)
		when day_of_week = 4
			and th in (0, 1)
			and wplace_type in (0, 1, 2)
			and dt between start_date
			and finish_date
		then (1 - th::int)
		when day_of_week = 5
			and fr in (0, 1)
			and wplace_type in (0, 1, 2)
			and wplace_type < 3
			and dt between start_date and finish_date
		then (1 - fr::int)
		-- mode 3
		when mo in (2, 3)
			and wplace_type = 3
			and mod(week, 2) = 1
			and dt between start_date and finish_date
			then 0
		when tu in (2, 3)
			and mod(week, 2) = 1
			and dt between start_date and finish_date
			then 0
		when we in (2, 3) 
			and mod(week, 2) = 1
			and dt between start_date and finish_date
			then 0
		when th in (2, 3)
			and mod(week, 2) = 1
			and dt between start_date and finish_date
			then 0
		when fr in (2, 3)
			and mod(week, 2) = 1
			and dt between start_date and finish_date
			then 0
		when mo in (2, 3)
			and wplace_type = 3
			and mod(week, 2) != 1
			and dt between start_date and finish_date
			then 1
		when tu in (2, 3)
			and mod(week, 2) != 1
			and dt between start_date and finish_date
			then 1
		when we in (2, 3) 
			and mod(week, 2) != 1
			and dt between start_date and finish_date
			then 1
		when th in (2, 3)
			and mod(week, 2) != 1
			and dt between start_date and finish_date
			then 1
		when fr in (2, 3)
			and mod(week, 2) != 1
			and dt between start_date and finish_date
			then 1
		when mo in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) = 1 or mod(week, 4) = 1)
			and dt between start_date and finish_date
			then 0
		when tu in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) = 1 or mod(week, 4) = 1)
			and dt between start_date and finish_date
			then 0
		when we in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) = 1 or mod(week, 4) = 1)
			and dt between start_date and finish_date
			then 0
		when th in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) = 1 or mod(week, 4) = 1)
			and dt between start_date and finish_date
			then 0
		when fr in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) = 1 or mod(week, 4) = 1)
			and dt between start_date and finish_date
			then 0
		when mo in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) != 1 or mod(week, 4) != 1)
			and dt between start_date and finish_date
			then 1
		when tu in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) != 1 or mod(week, 4) != 1)
			and dt between start_date and finish_date
			then 1
		when we in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) != 1 or mod(week, 4) != 1)
			and dt between start_date and finish_date
			then 1
		when th in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) != 1 or mod(week, 4) != 1)
			and dt between start_date and finish_date
			then 1
		when fr in (2, 3)
			and wplace_type = 4
			and (mod(week, 3) != 1 or mod(week, 4) != 1)
			and dt between start_date and finish_date
			then 1
		else null
	end) as to_be_at_office
from table_4)
select tab_num, dt as ymd_date, sum(to_be_at_office) as to_be_at_office
	from prefinal
group by tab_num, dt