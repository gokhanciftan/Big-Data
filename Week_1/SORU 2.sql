WITH t AS(
  SELECT 
  trips.start_station_id,
  trips.end_station_id,
  CASE WHEN (CAST(st.station_id AS INT)=start_station_id)THEN st.short_name END AS short_name,
  CASE WHEN (CAST(e.station_id AS INT)=end_station_id)THEN e.short_name END AS short_name_1,
  AVG(CASE WHEN member_gender='Male'THEN duration_sec END) AS male_avg_duration, 
  AVG(CASE WHEN member_gender='Female'THEN duration_sec END) AS female_avg_duration 
  FROM  bigquery-public-data.san_francisco_bikeshare.bikeshare_trips AS trips 
  JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS st
  ON CAST(st.station_id AS INT)=start_station_id
  JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS e
  ON CAST(e.station_id AS INT) = end_station_id
  GROUP BY start_station_id,end_station_id,short_name,short_name_1
  )
SELECT
start_station_id,
short_name ,
end_station_id,
short_name_1,
male_avg_duration ,
female_avg_duration
FROM t 
WHERE 
male_avg_duration IS NOT NULL AND
female_avg_duration IS NOT NULL



 