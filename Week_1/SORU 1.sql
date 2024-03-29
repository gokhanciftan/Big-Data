
SELECT regions.name,SUM(capacity) AS total_capacity 
FROM bigquery-public-data.san_francisco_bikeshare.bikeshare_regions AS regions
LEFT JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS info
ON regions.region_id=info.region_id
GROUP BY info.region_id,regions.name
HAVING total_capacity<5000

  