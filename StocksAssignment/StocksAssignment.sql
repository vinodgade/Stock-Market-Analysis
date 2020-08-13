create database assignment;
use assignment;

######### Creating temporary tables for converting Date variable to date format ########
create table bajajcon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from bajaj_auto;
create table eichercon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from eicher_motors;
create table herocon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from hero_motocorp;
create table infosyscon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from infosys;
create table tcscon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from tcs;
create table tvscon
select str_to_date(Date,'%d-%M-%Y') Date,`Close_Price` from tvs_motors;


### 1 Calculating the 20 and 50 Days Moving Average ###

create table bajaj1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from bajajcon
window w as (order by Date asc);
select * from bajaj1;

########For Eicher #####
create table eicher1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from eichercon
window w as (order by Date asc);
select * from eicher1;

#### For Hero motocorp ####
create table hero1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from herocon
window w as (order by Date asc);
select * from hero1;

#### For Infosys ####
create table infosys1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from infosyscon
window w as (order by Date asc);
select * from infosys1;

#### For TCS ####
create table tcs1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from tcscon
window w as (order by Date asc);
select * from tcs1;

#### For TVS motors ####
create table tvs1
select row_number() over w as Day, Date,`Close_Price`,
if((ROW_NUMBER() OVER w) > 19, (avg(`Close_Price`) OVER (order by Date asc rows 19 PRECEDING)), null) `20 Day MA`,
if((ROW_NUMBER() OVER w) > 49, (avg(`Close_Price`) OVER (order by Date asc rows 49 PRECEDING)), null) `50 Day MA`
from tvscon
window w as (order by Date asc);
select * from tvs1;

### 2 Master table creation by joining all the tables that are created in 1 ###

create table master_table 
select ba.`Date` as `Date`,ba.`Close_Price` as Bajaj, ei.`Close_Price` as Eicher, he.`Close_Price` as Hero, inf.`Close_Price` as Infosys, 
tc.`Close_Price` as TCS, tv.`Close_Price` as TVS
from bajaj1 ba
left join eicher1 ei on ba.Date = ei.Date 
left join hero1 he on ba.Date = he.Date
left join infosys1 inf on ba.Date = inf.Date
left join tcs1 tc on ba.Date = tc.Date
left join tvs1 tv on ba.Date = tv.Date;

select * from master_table;


#### 3 Generating BUY/SELL/HOLD signal tables #####

create table bajaj_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from bajaj1
window w as (order by Day);
select * from bajaj_pre;

create table eicher_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from eicher_pre
window w as (order by Day);
select * from eicher_pre;

create table hero_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from hero1
window w as (order by Day);
select * from hero_pre;

create table infosys_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from infosys1
window w as (order by Day);
select * from infosys_pre;

create table tcs_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from tcs1
window w as (order by Day);
select * from tcs_pre;

create table tvs_pre
select Day, Date, `Close_Price`, `20 Day MA`, lag(`20 Day MA`,1) over w as `20_MA_previous`, `50 Day MA`, lag(`50 Day MA`,1) over w as `50_MA_previous`
from tvs1
window w as (order by Day);
select * from tvs_pre;

############## Generating BUY/SELL/HOLD signal tables ##################

### For Bajaj ###
create table bajaj2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and 20_MA_previous < 50_MA_previous then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and 20_MA_previous > 50_MA_previous then 'SELL'
else 'HOLD' end) as 'Signal'
from bajaj_pre;
select * from bajaj2;

### For Eicher motors ###
create table eicher2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and `20_MA_previous` < `50_MA_previous` then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and `20_MA_previous` > `50_MA_previous` then 'SELL'
else 'HOLD' end) as 'Signal'
from eicher_pre;
select * from eicher2;

### For Hero motocorp ###
create table hero2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and `20_MA_previous` < `50_MA_previous` then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and `20_MA_previous` > `50_MA_previous` then 'SELL'
else 'HOLD' end) as 'Signal'
from hero_pre;
select * from hero2;

### For Infosys ###
create table infosys2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and `20_MA_previous` < `50_MA_previous` then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and `20_MA_previous` > `50_MA_previous` then 'SELL'
else 'HOLD' end) as 'Signal'
from infosys_pre;
select * from infosys2;

### For TCS ###
create table tcs2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and `20_MA_previous` < `50_MA_previous` then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and `20_MA_previous` > `50_MA_previous` then 'SELL'
else 'HOLD' end) as 'Signal'
from tcs_pre;
select * from tcs2;

### For TVS ###
create table tvs2
select Date,`Close_Price`,
(case when Day > 49 and `20 Day MA` > `50 Day MA` and `20_MA_previous` < `50_MA_previous` then 'BUY'
	 when Day > 49 and `20 Day MA` < `50 Day MA` and `20_MA_previous` > `50_MA_previous` then 'SELL'
else 'HOLD' end) as 'Signal'
from tvs_pre;
select * from tvs2;


######  4 UDF for taking date as input and return Signal of that day for Bajaj ##########

delimiter $$
create function input_date (d date)
returns char(50) deterministic
begin
declare s_value varchar(15);
set s_value = (select `Signal` from bajaj2 where Date = d);
return s_value;
end
$$
delimiter ;
select input_date('2016-09-14') as `Signal`;
