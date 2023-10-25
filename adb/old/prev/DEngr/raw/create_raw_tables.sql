-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS raw

-- COMMAND ----------

DROP TABLE IF EXISTS raw.circuits;
CREATE TABLE IF NOT EXISTS raw.circuits(
  circuitsId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/raw/circuits.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS raw.races;
CREATE TABLE IF NOT EXISTS raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/raw/races.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS raw.constructors;
CREATE TABLE IF NOT EXISTS raw.constructors
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/raw/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS raw.drivers;
CREATE TABLE IF NOT EXISTS raw.drivers
(
  driverId INT
  , driverRef STRING
  , number INT
  , code STRING
  , name STRUCT<forename: STRING, surname: STRING>
  , dob DATE
  , nationality STRING
  , url STRING
)
USING json
OPTIONS (path "/mnt/raw/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS raw.results;
CREATE TABLE IF NOT EXISTS raw.results
(
  resultId INT
  , raceId INT
  , driverId INT
  , constructorId INT
  , number INT
  , grid INT
  , position INT
  , positionText STRING
  , positionOrder INT
  , points INT
  , laps INT
  , time STRING
  , milliseconds INT
  , fastestLap INT
  , rank INT
  , fastestLapTime STRING
  , fastestLapSpeed FLOAT
  , statusId STRING
)
USING json
OPTIONS (path "/mnt/raw/results.json")

-- COMMAND ----------

DROP TABLE IF EXISTS raw.pit_stops;
CREATE TABLE IF NOT EXISTS raw.pit_stops
(
  driverId INT
  , duration STRING
  , lap INT
  , milliseconds INT
  , raceId INT
  , stop INT
  , time STRING
)
USING json
OPTIONS (path "/mnt/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT *
FROM raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS raw.lap_times;
CREATE TABLE IF NOT EXISTS raw.lap_times
(
  race_d INT
  , driverId INT
  , lap INT
  , position INT
  , time STRING
  , milliseconds INT
)
USING csv
OPTIONS (path "/mnt/raw/lap_times")

-- COMMAND ----------

DROP TABLE IF EXISTS raw.qualifying;
CREATE TABLE IF NOT EXISTS raw.qualifying
(
  constructorId INT
  , driverId INT
  , number INT
  , position INT
  , q1 STRING
  , q2 STRING
  , q3 STRING
  , qualifyId INT
  , raceId INT
)
USING json
OPTIONS (path "/mnt/raw/qualifying", multiLine true)

-- COMMAND ----------


