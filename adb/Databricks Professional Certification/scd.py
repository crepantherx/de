# Databricks notebook source
# MAGIC %sql CREATE DATABASE IF NOT EXISTS scd

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS scd.a (a INT, b STRING, c INT);
# MAGIC CREATE TABLE IF NOT EXISTS scd.b (a INT, b STRING, c INT);

# COMMAND ----------

# MAGIC %sql INSERT INTO scd.a VALUES
# MAGIC (1, 'a', 34534)
# MAGIC , (2, 'b', 4456)
# MAGIC , (3, 'c', 5656)

# COMMAND ----------

# MAGIC %sql INSERT INTO scd.b VALUES
# MAGIC (4, 'a', 34534)
# MAGIC , (2, 'b', 0)
# MAGIC , (5, 'c', 5656)

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd.a

# COMMAND ----------

# MAGIC %sql DROP TABLE scd.m

# COMMAND ----------

# MAGIC %sql CREATE TABLE IF NOT EXISTS scd.m (a INT, b STRING, c INT);

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC MERGE INTO scd.m
# MAGIC USING scd.a
# MAGIC ON scd.m.a = scd.a.a
# MAGIC WHEN MATCHED THEN UPDATE SET scd.m.c = scd.a.c
# MAGIC WHEN NOT MATCHED THEN INSERT (a,b,c) VALUES(scd.a.a, scd.a.b, scd.a.c);

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd.m

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC MERGE INTO scd.m
# MAGIC USING scd.b
# MAGIC ON scd.m.a = scd.b.a
# MAGIC WHEN MATCHED THEN UPDATE SET scd.m.c = scd.b.c
# MAGIC WHEN NOT MATCHED THEN INSERT (a,b,c) VALUES(scd.b.a, scd.b.b, scd.b.c);

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd.m

# COMMAND ----------

# MAGIC %sql DROP DATABASE scd2

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS scd2

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE scd2.appraisal

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS scd2.salary;
# MAGIC CREATE TABLE IF NOT EXISTS scd2.salary (
# MAGIC   `id` INT
# MAGIC   , name STRING
# MAGIC   , salary INT
# MAGIC   , `from_date` DATE
# MAGIC   , `to_date` DATE
# MAGIC   , active int
# MAGIC );
# MAGIC
# MAGIC TRUNCATE TABLE scd2.salary;
# MAGIC INSERT INTO scd2.salary VALUES
# MAGIC (1, 'a', 100, '2021-01-01', NULL, 1)
# MAGIC , (2, 'b', 86, '2021-02-01', NULL, 1)
# MAGIC , (3, 'c', 34, '2021-02-01', NULL, 1)
# MAGIC , (4, 'd', 54, '2021-02-01', NULL, 1)
# MAGIC , (5, 'e', 53, '2021-02-01', NULL, 1)
# MAGIC
# MAGIC -- (1, 'a', 23, '2021-01-01', NULL, 1);

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS scd2.appraisal (
# MAGIC   `id` INT
# MAGIC   , name STRING
# MAGIC   , salary INT
# MAGIC );

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql insert INTO scd2.appraisal values
# MAGIC (3, 'c', 99)
# MAGIC , (5, 'e', 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd2.salary USING scd2.appraisal 
# MAGIC ON scd2.salary.id = scd2.appraisal.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET to_date = curdate()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, name, salary, from_date, to_date) VALUES (scd2.appraisal.id, scd2.appraisal.name, scd2.appraisal.salary, curdate(), 1)

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd2.salary as a USING scd2.appraisal as b
# MAGIC ON a.id = b.id
# MAGIC WHEN MATCHED THEN UPDATE SET a.salary = b.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (id, name, `salary`, `from`, `to`, active) VALUES(b.id, b.name, b.salary, curdate(), NULL, 1)

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql SHOW TABLES IN scd2

# COMMAND ----------

# MAGIC %sql TRUNCATE TABLE scd2.salary

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.appraisal

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd2.salary as salary USING 
# MAGIC (
# MAGIC SELECT id as merge_key, scd2.appraisal.* FROM scd2.appraisal
# MAGIC   UNION ALL
# MAGIC   SELECT NULL as merge_key, scd2.appraisal.* FROM scd2.appraisal
# MAGIC   JOIN scd2.salary ON scd2.salary.id = scd2.appraisal.id
# MAGIC   WHERE scd2.salary.salary <> scd2.appraisal.salary and scd2.salary.active=1
# MAGIC ) AS appraisal
# MAGIC ON salary.id = merge_key
# MAGIC WHEN MATCHED AND salary.salary <> appraisal.salary AND salary.active=1 THEN 
# MAGIC   UPDATE 
# MAGIC     SET salary.to_date = curdate(), salary.active=0
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, name, salary, from_date, to_date, active) VALUES (appraisal.id, appraisal.name, appraisal.salary, curdate(), NULL, 1)

# COMMAND ----------

# MAGIC %sql   SELECT id as merge_key, scd2.appraisal.* FROM scd2.appraisal
# MAGIC   UNION ALL
# MAGIC   SELECT NULL as merge_key, scd2.appraisal.* FROM scd2.appraisal
# MAGIC   JOIN scd2.salary ON scd2.salary.id = scd2.appraisal.id
# MAGIC   WHERE scd2.salary.salary <> scd2.appraisal.salary

# COMMAND ----------

# MAGIC %sql SELECT * FROM scd2.salary

# COMMAND ----------


