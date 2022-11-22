
-- upload data to the s3 bucket
aws s3 cp s3://hiveassignmentdatabde/Parking_Violations_Issued_-_Fiscal_Year_2017.csv s3://myhiveproject-rimi/parking/



create databse parking;

-- create table
CREATE EXTERNAL TABLE IF NOT EXISTS parking_violations(
`SummonsNumber` bigint,
`PlateID` string,
`RegistrationState` string,
`PlateType` string,
`IssueDate` string,
`ViolationCode` int,
`VehicleBodyType` string,
`VehicleMake` string,
`IssuingAgency` string,
`StreetCode1` int ,
`StreetCode2` int,
`StreetCode3` int,
`VehicleExpirationDate` int,
`ViolationLocation` string,
`ViolationPrecinct` int,
`IssuerPrecinct` int,
`IssuerCode` bigint,
`IssuerCommand` string,
`IssuerSquad` string,
`ViolationTime` string,
`TimeFirstObserved` string,
`ViolationCounty` string,
`ViolationInFrontOfOrOpposite` string,
`HouseNumber` string,
`StreetName` string,
`IntersectingStreet` string,
`DateFirstObserved` int,
`LawSection` int,
`SubDivision` string,
`ViolationLegalCode` string,
`DaysParkingInEffect` string,
`FromHoursInEffect` string,
`ToHoursInEffect` string,
`VehicleColor` string,
`UnregisteredVehicle` string,
`VehicleYear` int,
`MeterNumber` string,
`FeetFromCurb` int,
`ViolationPostCode` string,
`ViolationDescription` string,
`NoStandingorStoppingViolation` string,
`HydrantViolation` string,
`DoubleParkingViolation` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
LOCATION 's3://myhiveproject-rimi/parking/'
TBLPROPERTIES ( "skip.header.line.count"="1"); 


set hive.cli.print.header=true;




-- create optimised ORC table


CREATE EXTERNAL TABLE IF NOT EXISTS parking_violations_ORC(
`SummonsNumber` bigint,
`PlateID` string,
`RegistrationState` string,
`PlateType` string,
`IssueDate` string,
`ViolationCode` int,
`VehicleBodyType` string,
`VehicleMake` string,
`IssuingAgency` string,
`StreetCode1` int ,
`StreetCode2` int,
`StreetCode3` int,
`VehicleExpirationDate` int,
`ViolationLocation` string,
`ViolationPrecinct` int,
`IssuerPrecinct` int,
`IssuerCode` bigint,
`IssuerCommand` string,
`IssuerSquad` string,
`ViolationTime` string,
`TimeFirstObserved` string,
`ViolationCounty` string,
`ViolationInFrontOfOrOpposite` string,
`HouseNumber` string,
`StreetName` string,
`IntersectingStreet` string,
`DateFirstObserved` int,
`LawSection` int,
`SubDivision` string,
`ViolationLegalCode` string,
`DaysParkingInEffect` string,
`FromHoursInEffect` string,
`ToHoursInEffect` string,
`VehicleColor` string,
`UnregisteredVehicle` string,
`VehicleYear` int,
`MeterNumber` string,
`FeetFromCurb` int,
`ViolationPostCode` string,
`ViolationDescription` string,
`NoStandingorStoppingViolation` string,
`HydrantViolation` string,
`DoubleParkingViolation` string
)
partitioned by (month string )
STORED AS ORC
LOCATION 's3://myhiveproject-rimi/parking/'
TBLPROPERTIES ( 'orc.compress'='SNAPPY'); 


-- set partition properties
set hive.exec.dynamic.partition.mode=nonstrict;

-- insert data to OCR table 
INSERT OVERWRITE TABLE parking_violations_ORC PARTITION(month)
SELECT *, CONCAT(SUBSTR(`ISSUEDATE`,7,4), SUBSTR(`ISSUEDATE`,1,2)) AS MONTH
FROM parking_violations
where (`ISSUEDATE` like '%2017')
;

-- Part-I: Examine the data


-- Find the total number of tickets for the year. 1127158
select count(SummonsNumber) AS No_Of_Tickets from parking_violations_ORC;

-- Find out the total number of states to which the cars with tickets belong. The count of states is mandatory here, providing the exact list of states is optional.
-- 65
select count(distinct `RegistrationState`)
from parking_violations_ORC;

select distinct `RegistrationState`
from parking_violations_ORC;

-- Some parking tickets don’t have addresses on them, which is a cause for concern. Find out the number of such tickets, which have no addresses. 
-- (i.e. tickets where one of the Street Codes, i.e. "Street Code 1" or "Street Code 2" or "Street Code 3" is empty)
/*
1416762061
1418938300
1422528194
1416450725
*/
select SummonsNumber from parking_violations_ORC
where StreetCode1  is null or 
StreetCode2 is null or 
StreetCode3   is null ;


-- Part-II: Aggregation tasks

-- What are the top 5 most frequently occurring violation codes? 
-- (Note that frequency means the number of occurrences over a time period. The list should be in descending order)
/*21	768082
36	662765
38	542079
14	476660
20	319646*/

select * from
(
select ViolationCode, count(ViolationCode) as cnt
from parking_violations_ORC
group by ViolationCode) temp
 order by cnt desc
limit 5;

-- How often does each vehicle body type get a parking ticket? 
/* 
SUBN	1883953
4DSD	1547307
VAN	724025
DELV	358982
SDN	194197*/
select * from (
select VehicleBodyType, count(VehicleBodyType) as cnt
from parking_violations_ORC
group by VehicleBodyType)temp
order by cnt desc
limit 5;

-- How about the vehicle make? (List the top 5 for both)
/*
FORD	636842
TOYOT	605290
HONDA	538884
NISSA	462017
CHEVR	356032
*/
select * from (
select VehicleMake, count(VehicleMake) as cnt
from parking_violations_ORC
group by VehicleMake
)temp
order by cnt desc
limit 5;

/*A precinct is a police station that has a certain zone of the city under its command. 
You will find two further classifications of precincts:
Violating Precincts - These are precincts where the violations have occurred.
Issuer Precincts - These are precincts that issued the tickets.
Find the top 5 Violating Precincts and Issuer Precincts by frequency.*/


/*
*/

select * from (
select ViolationPrecinct, count(ViolationPrecinct) as cnt
from parking_violations_ORC
group by ViolationPrecinct
)temp
order by cnt desc
limit 5;

/*
0	925596
19	274443
14	203552
1	174702
18	169131
*/

select * from (
select IssuerPrecinct, count(IssuerPrecinct) as cnt
from parking_violations_ORC
group by IssuerPrecinct
)temp
order by cnt desc
limit 5;



/*
Find the violation code frequency across the top 3 precincts which have issued the highest number of tickets.
 Do these precinct zones have an exceptionally high frequency of certain violation codes? 
 If yes, list them.
*/
select substring(ViolationTime, 1,2), 
count(*) as violationsCountINAM 
from parking_violations_ORC 
where 
upper(substring(ViolationTime, -1)) ='A' 
group by substring(ViolationTime, 1, 2) 
order by violationsCountINAM desc;

select substring(ViolationTime, 1,2), 
count(*) as violationcountINPM 
from parking_violations_ORC 
where upper(substring(ViolationTime, -1)) ='P' 
group by substring(ViolationTime, 1, 2) 
order by ViolationCountINPM desc;


/*
Find out the frequency of parking violations across different times of the day: 
The Violation Time field is specified in a strange format. 
Find a way to make this into a time attribute that you can use to divide into groups.

Divide 24 hours into 6 equal discrete bins of time. 
The intervals you choose are at your discretion. 
For each of these groups, find the 3 most commonly occurring violations.
*/
-- we will devide the data into 4 hours slots
select * from (
select violationbin,violationcode,ViolationCount, 
dense_rank() over (partition by violationbin order by ViolationCount desc) as rank 
from 
( Select violationbin, ViolationCode, count(*) as ViolationCount from 
( select case 
when substring(violationtime,1,2) in ('00','12','01','02','03') and upper(substring(violationtime,-1))='A' then 'MidNight_12AM_3AM' 
when substring(violationtime,1,2) in ('04','05','06','07') and upper(substring(violationtime,-1))='A' then 'EarlyMorning_4AM_7AM' 
when substring(violationtime,1,2) in ('08','09','10','11') and upper(substring(violationtime,-1))='A' then 'Morning_8AM_11AM' 
when substring(violationtime,1,2) in ('12','01','02','03') and upper(substring(violationtime,-1))='P' then 'AfterNoon_12PM_3PM'
when substring(violationtime,1,2) in ('04','05','06','07') and upper(substring(violationtime,-1))='P' then 'Evening_4PM_7PM'
when substring(violationtime,1,2) in ('08','09','10','11') and upper(substring(violationtime,-1))='P' then 'Night_8PM_11PM' 
else null end as violationbin, 
ViolationCode 
from parking_violations_ORC 
)temp1
where violationbin is not NULL 
group by violationbin,ViolationCode
) temp2 
) temp3 where rank <= 3 ;



/*
Now, try another direction. For the 3 most commonly occurring violation codes, 
find the most common times of day (in terms of the bins from the previous part).
*/


/*
Let’s try and find some seasonality in this data:
First, divide the year into seasons, and find frequencies of tickets for each season. (
Hint: A quick Google search reveals the following seasons in NYC: 
Spring(March, April, May); 
Summer(June, July, August); 
Fall(September, October, November); 
Winter(December, January, February))
*/


select *,count(SummonsNumber)  as cnt from
(
select SummonsNumber, case when substr(month, 5, 2) in ('03', '04', '05') then 'spring'
when substr(month, 5, 2) in ('06', '07', '08') then 'Summer'
when substr(month, 5, 2) in ('09', '10', '11') then 'Fall'
when substr(month, 5, 2) in ('12', '01', '02') then 'Winter'
end as season
from parking_violations_ORC)temp
group by season;



-- Then, find the 3 most common violations for each of these seasons.

select * from 
(select season, ViolationCode, dense_rank() over(partition by season order by cnt) as ranks
from 
(select *,count(SummonsNumber)  as cnt from
(
select SummonsNumber, ViolationCode,  
case when substr(month, 5, 2) in ('03', '04', '05') then 'spring'
when substr(month, 5, 2) in ('06', '07', '08') then 'Summer'
when substr(month, 5, 2) in ('09', '10', '11') then 'Fall'
when substr(month, 5, 2) in ('12', '01', '02') then 'Winter'
end as season
from parking_violations_ORC)temp
group by season, ViolationCode) temp2
)temp3
where ranks<=3

