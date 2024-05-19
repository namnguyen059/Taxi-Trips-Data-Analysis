-- Batch dashboard
CREATE OR REPLACE TABLE `taxi-418810.taxi_records.march10` AS (
SELECT 
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM 
    `taxi-418810.taxi_records.fact_table` f
JOIN 
    `taxi-418810.taxi_records.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN 
    `taxi-418810.taxi_records.passenger_count_dim` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN 
    `taxi-418810.taxi_records.trip_distance_dim` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN 
    `taxi-418810.taxi_records.rate_code_dim` r ON r.rate_code_id=f.rate_code_id  
JOIN 
    `taxi-418810.taxi_records.pickup_location_dim` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN 
    `taxi-418810.taxi_records.dropoff_location_dim` drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN 
    `taxi-418810.taxi_records.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id
WHERE 
    DATE(d.tpep_pickup_datetime) = '2016-03-10'
);

-- Fare over week and payment
SELECT MAX(trip_distance) FROM taxi-418810.taxi_records.march10;

SELECT 
    d.pick_weekday,
    p.payment_type_name,
    SUM(f.fare_amount) AS total_fare_amount
FROM 
    taxi-418810.taxi_records.fact_table f
JOIN 
    taxi-418810.taxi_records.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN 
    taxi-418810.taxi_records.payment_type_dim p ON f.payment_type_id = p.payment_type_id
GROUP BY 
    d.pick_weekday, p.payment_type_name
ORDER BY 
    d.pick_weekday, p.payment_type_name;


-- top 10 pickup locations based on the number of trips
SELECT pickup_location_id, COUNT(trip_id) as No_of_Trips
FROM uber_dataset.fact_table
GROUP BY pickup_location_id
ORDER BY No_of_Trips DESC
LIMIT 10;

-- the total number of trips by passenger count
SELECT passenger_count, COUNT(passenger_count) AS No_of_Trips
FROM uber-big-data-analysis.uber_dataset.passenger_count_dim 
GROUP BY passenger_count;

-- Average fare amount by the hour of the day
SELECT d.pick_hour, AVG(f.fare_amount) AS Avg_Fare_Amt 
FROM uber-big-data-analysis.uber_dataset.datetime_dim d
JOIN uber-big-data-analysis.uber_dataset.fact_table f
ON d.datetime_id=f.datetime_id
GROUP BY d.pick_hour
ORDER BY AVG(f.fare_amount) DESC;



-- Streaming dashboard
SELECT * FROM taxirides.realtime LIMIT 10

WITH streaming_data AS (

SELECT
  timestamp,
  TIMESTAMP_TRUNC(timestamp, HOUR, 'UTC') AS hour,
  TIMESTAMP_TRUNC(timestamp, MINUTE, 'UTC') AS minute,
  TIMESTAMP_TRUNC(timestamp, SECOND, 'UTC') AS second,
  ride_id,
  latitude,
  longitude,
  meter_reading,
  ride_status,
  passenger_count
FROM
  taxirides.realtime
ORDER BY timestamp 
LIMIT 950

)
SELECT
 ROW_NUMBER() OVER() AS dashboard_sort,
 minute,
  latitude,
  longitude,
 COUNT(DISTINCT ride_id) AS total_rides,
 SUM(meter_reading) AS total_revenue,
 SUM(passenger_count) AS total_passengers
FROM streaming_data
GROUP BY minute, latitude,
  longitude, timestamp