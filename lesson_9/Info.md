root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)

(VendorID: Int,
tpep_pickup_datetime: Timestamp,
tpep_dropoff_datetime: Timestamp,
passenger_count: Int,
trip_distance: Double,
RatecodeID: Int,
store_and_fwd_flag: String,
PULocationID: Int,
DOLocationID: Int,
payment_type: Int,
fare_amount: Double,
extra: Double,
mta_tax: Double,
tip_amount: Double,
tolls_amount: Double,
improvement_surcharge: Double,
total_amount: Double)

+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
|       2| 2018-01-24 18:02:56|  2018-01-24 18:10:58|              1|         2.02|         1|                 N|          48|         107|           2|        8.5|  0.5|    0.5|       0.0|         0.0|                  0.3|         9.8|
|       2| 2018-01-24 18:57:13|  2018-01-24 19:21:17|              1|        10.13|         1|                 N|          79|         244|           2|       28.5|  0.5|    0.5|       0.0|         0.0|                  0.3|        29.8|
|       2| 2018-01-24 20:29:32|  2018-01-24 20:41:29|              1|         3.38|         1|                 N|         239|          48|           1|       13.0|  0.0|    0.5|      2.76|         0.0|                  0.3|       16.56|
|       2| 2018-01-24 20:43:47|  2018-01-24 21:10:50|              1|          8.6|         1|                 N|          48|          36|           1|       28.0|  0.0|    0.5|       7.2|         0.0|                  0.3|        36.0|
|       2| 2018-01-24 21:36:35|  2018-01-24 21:41:39|              1|         1.24|         1|                 N|         170|         107|           2|        6.0|  0.0|    0.5|       0.0|         0.0|                  0.3|         6.8|
|       2| 2018-01-25 00:14:21|  2018-01-25 00:30:04|              2|         2.47|         1|                 N|         151|         238|           1|       12.5|  0.0|    0.5|      2.66|         0.0|                  0.3|       15.96|
|       2| 2018-01-25 00:31:54|  2018-01-25 00:40:26|              1|         1.53|         1|                 N|         238|         143|           1|        8.0|  0.0|    0.5|      1.32|         0.0|                  0.3|       10.12|
|       2| 2018-01-25 00:45:31|  2018-01-25 00:59:40|              2|          1.9|         1|                 N|         239|         237|           1|       11.0|  0.0|    0.5|      2.36|         0.0|                  0.3|       14.16|
|       2| 2018-01-25 01:05:28|  2018-01-25 01:10:12|              1|          0.6|         1|                 N|         141|         237|           1|        5.0|  0.0|    0.5|      1.16|         0.0|                  0.3|        6.96|
|       2| 2018-01-25 01:11:49|  2018-01-25 01:17:53|              1|         0.66|         1|                 N|         236|         236|           1|        5.5|  0.0|    0.5|      1.26|         0.0|                  0.3|        7.56|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
