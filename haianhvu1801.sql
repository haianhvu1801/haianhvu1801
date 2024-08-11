SELECT *
FROM US_Household_Income;

#1.**Data Consistency**:
##Check for and correct inconsistencies.
SELECT Type
FROM US_Household_Income
GROUP BY Type;

SET SQL_SAFE_UPDATES = 0;
UPDATE US_Household_Income
SET `Type` = 'CDP'
WHERE `Type` = 'CPD';

UPDATE US_Household_Income
SET `Type` = 'Borough'
WHERE `Type` = 'Boroughs';

#2.**Removing Duplicates**:
##Identify and remove duplicate rows if any.
SELECT *
FROM (
SELECT row_id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) as row_num 
FROM us_household_income) as row_table
WHERE row_num > 1;

DELETE FROM us_household_income
WHERE row_id IN (
	SELECT row_id
	FROM (
	SELECT row_id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) as row_num 
	FROM us_household_income) as row_table
	WHERE row_num > 1
    );
#RESULT: Delete 7 duplicate rows

#Task 1: Summarizing Data by State
SELECT State_Name, State_ab, AVG(ALand) as avg_land, AVG(AWater) as avg_water
FROM us_household_income
GROUP BY State_Name, State_ab
ORDER BY AVG(ALand) DESC;
#RESULT: 53 rows returned

#Task 2: Filtering Cities by Population Range
SELECT City, State_Name, County
FROM us_household_income
WHERE ALand > 50000000 AND Aland < 100000000
ORDER BY City;
#RESULT: 1841 rows returned

#Task 3: Counting Cities per State
SELECT State_Name, State_ab, COUNT(City) AS num_cities
FROM us_household_income
GROUP BY State_Name, State_ab
ORDER BY num_cities DESC;
#RESULT: 53 rows returned

#Task 4: Identifying Counties with Significant Water Area
SELECT County, State_Name,SUM(Awater) AS total_water_area
FROM us_household_income
GROUP BY County, State_Name
ORDER BY total_water_area DESC
LIMIT 10;
#RESULT: 10 rows returned

#Task 5:  Finding Cities Near Specific Coordinates
SELECT City, State_Name, County, Lat, Lon
FROM us_household_income
WHERE Lat BETWEEN 30 AND 35 AND Lon BETWEEN -90 AND -85
ORDER BY Lat, Lon;
#RESULT: 893 rows returned

#Task 6: Using Window Functions for Ranking
SELECT City, State_Name, Aland,
RANK() OVER(PARTITION BY City ORDER BY Aland DESC) as rank_cities
FROM us_household_income
ORDER BY State_Name, rank_cities; 
#RESULT: 32526 rows returned

#Task 7: Creating Aggregate Reports
SELECT 
    State_Name,
    State_ab,
    SUM(Aland) AS total_land,
    SUM(Awater) AS total_water,
    COUNT(City) AS num_cities
FROM
    us_household_income
GROUP BY State_Name,State_ab
ORDER BY total_land DESC;
#RESULT: 53 rows returned

#Task 8: Subqueries for Detailed Analysis
SELECT City, State_Name, Aland
FROM us_household_income
WHERE Aland > (SELECT AVG(Aland) FROM us_household_income)
ORDER BY Aland DESC;
#RESULT: 4577 rows returned

#Task 9: Identifying Cities with High Water to Land Ratios
SELECT City, State_Name, Aland, Awater, Aland/ Awater AS water_to_land_ratio
FROM us_household_income
WHERE AWater > 0.5*ALand
ORDER BY water_to_land_ratio DESC;
#RESULT: 955 rows returned

#Task 10: Dynamic SQL for Custom Reports
DELIMITER $$
CREATE PROCEDURE custom_reports(IN p_state_ab CHAR(2))
BEGIN
    DECLARE total_cities INT;
    DECLARE avg_water FLOAT;
    DECLARE avg_land FLOAT;

    SELECT COUNT(City)
    INTO total_cities
    FROM us_household_income
    WHERE State_ab = p_state_ab;

    SELECT AVG(AWater)
    INTO avg_water
    FROM us_household_income
    WHERE State_ab = p_state_ab;

    SELECT AVG(Aland)
    INTO avg_land
    FROM us_household_income
    WHERE State_ab = p_state_ab;

    SELECT total_cities, avg_water, avg_land;
END$$
DELIMITER ;

CALL custom_reports('IA'); 
#RESULT: 1 row returned:total_cities ='454',avg_water ='1126660',avg_land ='143013000'
   
#Task 11: Creating and Using Temporary Tables
DROP TEMPORARY TABLE top_20_cities; 
CREATE TEMPORARY TABLE top_20_cities 
	(City VARCHAR(100),
	State_Name VARCHAR(100),
	Aland FLOAT,
	Awater FLOAT);
INSERT INTO top_20_cities (City,State_Name, Aland, Awater)
SELECT City, State_Name, Aland, Awater
FROM us_household_income
ORDER BY Aland DESC
LIMIT 20;

SELECT AVG(Awater) AS avg_water
FROM top_20_cities; #RESULT: 4418570489.8

SELECT City,State_Name, Aland, Awater
FROM top_20_cities; # 20 rows returned

#Task 12: Complex Multi-Level Subqueries
SELECT State_Name, AVG(Aland) AS Average_Land_Area
FROM us_household_income
GROUP BY State_Name
HAVING AVG(Aland) > (
  SELECT AVG(Aland)
  FROM us_household_income
);
#RESULT: 20 rows returned

#Task 13:Optimizing Indexes for Query Performance
EXPLAIN SELECT *
FROM us_household_income
WHERE State_Name = 'Alabama';
#Before using INDEX, we have to scan 32477 rows and the percentage of the table rows that gonna be filtered is 10.00
CREATE INDEX idx_state_name
ON us_household_income (State_Name);
#After using INDEX, we just have to scan 526 rows and the percentage of the table rows that gonna filtered is 100.00

#Create index for city
CREATE INDEX idx_city
ON us_household_income (City);

#Create index for county
CREATE INDEX idx_county
ON us_household_income (County);

#Task 14:Data Anomalies Detection
SELECT City, State_Name, Aland, StateAvgLandArea, (Aland - StateAvgLandArea) / StdDevLandArea AS ZScore
FROM (
  SELECT City, State_Name, Aland,
         AVG(Aland) OVER (PARTITION BY State_Name) AS StateAvgLandArea,
         STDDEV(Aland) OVER (PARTITION BY State_Name) AS StdDevLandArea
  FROM US_household_income
) AS LandAreaData
ORDER BY ZScore DESC;
#RESULT: 32526 rows returned

#Task 15: Implementing Triggers for Data Integrity
DELIMITER $$
CREATE TRIGGER update_us_household_income
AFTER INSERT ON us_household_income
FOR EACH ROW
BEGIN
  UPDATE us_household_income
  SET total_land = total_land + NEW.Aland,
      total_water = total_water + NEW.Awater
  WHERE State_Name = NEW.State_Name;
END $$
DELIMITER ;
	
#Task 16: Analyzing Correlations

WITH table_mean AS (
  select avg(Aland) as mean_land,
         avg(Awater) as mean_water
  from us_household_income
),

table_corrected AS (
  select Aland - mean_land as mean_land_corrected,
         Awater - mean_water as mean_water_corrected
  from table_mean, us_household_income
),

table_variance AS (
  SELECT  sum(mean_land_corrected * mean_land_corrected) AS var_land,
          sum(mean_water_corrected * mean_water_corrected) AS var_water,
          sum(mean_land_corrected * mean_water_corrected) as numerator
  FROM table_corrected
)
SELECT numerator/sqrt(var_land * var_water) AS r
FROM table_variance;
#RESULT: R= 0.5586..

#THANK YOU FOR TAKING TIME TO READ <3
