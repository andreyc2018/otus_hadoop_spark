
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;

\connect otus

CREATE TABLE trip_stats (
    trips       INT,
    avg_dist    REAL,
    max_dist    REAL,
    min_dist    REAL,
    stddev_dist REAL
);
