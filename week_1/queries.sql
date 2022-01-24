select count(*) from yellow_taxi_trips where date_part('month', "tpep_pickup_datetime")=1 and date_part('day', "tpep_pickup_datetime")=15

select date_part('day', "tpep_pickup_datetime"), max("tip_amount") as tip from yellow_taxi_trips
where date_part('month', "tpep_pickup_datetime")=1
group by date_part('day', "tpep_pickup_datetime")
order by tip desc

select
	D."Zone",
	count(*) as total
from
	yellow_taxi_trips as trips
join zones as PU
	on trips."PULocationID"=PU."LocationID"
join zones as D
	on trips."DOLocationID"=D."LocationID"
where
	PU."Zone"='Central Park' and
	date_part('day', trips."tpep_pickup_datetime")=14 and
	date_part('month', trips."tpep_pickup_datetime")=1
group by D."Zone"
order by total desc

select
	PU."Zone",
	D."Zone",
	avg(total_amount) as average_price
from
	yellow_taxi_trips as trips
join zones as PU
	on trips."PULocationID"=PU."LocationID"
join zones as D
	on trips."DOLocationID"=D."LocationID"
group by
	PU."Zone",
	D."Zone"
order by average_price desc
