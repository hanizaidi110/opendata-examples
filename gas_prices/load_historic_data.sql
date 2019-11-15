CREATE OR REPLACE SCRIPT &STAGESCM..LOAD_GAS_PRICE_DATA AS

local date_table = os.date("*t")
local c_year,c_month,c_day = date_table.year, date_table.month, date_table.day-1

-- add trailing zero to month and day if length is one
if string.len(c_month) < 2 then
	c_month = "0"..c_month
end
if string.len(c_day) < 2 then
	c_day = "0"..c_day 
end

----***STATIONS***-----
-- delete all previous data from stations staging table
query([[truncate table &STAGESCM..STATIONS]])

--import new data in stations staging table
import_sql_stat = "import into &STAGESCM..STATIONS from csv at 'https://dev.azure.com/tankerkoenig/362e70d1-bafa-4cf7-a346-1f3613304973/_apis/git/repositories/0d6e7286-91e4-402c-af56-fa75be1f223d/Items?path=/stations/"..c_year.."/"..c_month.."/' file '"..c_year.."-"..c_month.."-"..c_day.."-stations.csv' (1..9,10 FORMAT='YYYY-MM-DD HH:MI:SS',11)  COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' row separator = 'LF' skip = 1 REJECT LIMIT 0;" 
sucess, resTable = pquery(import_sql_stat)
if sucess then 
	output('Stations Data imported successfully')	
else 
	output ("Failed to import: " .. resTable.error_message .. " \nCaused by statement: ".. resTable.statement_text)
end

-- merge into production table(think of possible issues)

merge_sql_stat = "merge into &PRODSCM..STATIONS tgt using &STAGESCM..STATIONS src on TGT.UUID = src.UUID when not matched then insert (UUID,NAME,BRAND,STREET,HOUSE_NUMBER,POST_CODE,CITY,LATITUDE,LONGITUDE,FIRST_ACTIVE) values (UUID,NAME,BRAND,STREET,HOUSE_NUMBER,POST_CODE,CITY,LATITUDE,LONGITUDE,FIRST_ACTIVE);"
suc, resT = pquery(merge_sql_stat)
if suc then 
	output('Stations Data merged successfully in Production table')	
else 
	output ("Failed to merge: " .. resT.error_message .. " \nCaused by statement: ".. resT.statement_text)
end

-- solve for opening times
-- insert all new opening times in opening_periods prod table
query([[
insert into &PRODSCM..OPENING_PERIODS(UUID,APPLICABLE_DAYS,START_TIME,END_TIME) 
(with tmp as
(SELECT UUID,OPENINGTIMES_JSON FROM &STAGESCM..STATIONS WHERE UUID NOT IN (SELECT UUID FROM &PRODSCM..STATIONS))
SELECT UUID,&STAGESCM..json_parsing_time_periods(OPENINGTIMES_JSON) FROM TMP);]])

-- insert all new closing times in closing_periods prod table
----
query(
[[insert into &PRODSCM..CLOSING_PERIODS(UUID,START_TIME,END_TIME) 
(with tmp as
(SELECT UUID,OPENINGTIMES_JSON FROM &STAGESCM..STATIONS WHERE UUID NOT IN (SELECT UUID FROM &PRODSCM..STATIONS))
SELECT UUID,&STAGESCM..json_parsing_closing_periods(OPENINGTIMES_JSON) FROM TMP);]]
)
---- 

----***PRICES***-----
-- delete all previous data from pricing staging table
query([[truncate table &STAGESCM..PRICES]])

-- insert check making sure data is not added twice
_,restab = pquery("SELECT last_load_date from &STAGESCM..loading_dates")
if "'"..c_year.."-"..c_month.."-"..c_day.."'" == "'"..tostring(restab[1][1]).."'" then
	output("Gas Price data for the following date "..tostring(restab[1][1]).." already exists") 
else 
	-- import new data in pricing staging table
	import_sql_prices = "import into &STAGESCM..PRICES from csv at 'https://dev.azure.com/tankerkoenig/362e70d1-bafa-4cf7-a346-1f3613304973/_apis/git/repositories/0d6e7286-91e4-402c-af56-fa75be1f223d/Items?path=/prices/"..c_year.."/"..c_month.."/' file '"..c_year.."-"..c_month.."-"..c_day.."-prices.csv' (1 FORMAT='YYYY-MM-DD HH:MI:SS',2..8) COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' row separator = 'LF' skip = 1 REJECT LIMIT 0;" 
	suc, res = pquery(import_sql_prices)
	if suc then 
		output('Prices Data imported successfully')	
	else 
		output ("Failed to import: " .. res.error_message .. " \nCaused by statement: ".. resTable.statement_text)
	end

	-- insert into production table (think of possible issues)
	insert_sql_prices = "insert into &PRODSCM..PRICES(MODIFICATION_TIME, UUID, DIESEL, E5, E10, DIESEL_CHANGE, E5_CHANGE, E10_CHANGE) select MODIFICATION_TIME, UUID, DIESEL, E5, E10, DIESEL_CHANGE, E5_CHANGE, E10_CHANGE from &STAGESCM..PRICES WHERE UUID IN (SELECT UUID FROM &PRODSCM..STATIONS);"
	s,re = pquery(insert_sql_prices)
	if s then 
		output('New Prices Data inserted successfully in Prices table')	
	else 
		output ("Failed to insert: " .. re.error_message .. " \nCaused by statement: ".. re.statement_text)
	end
	
	-- update loading_dates 
	query("UPDATE &STAGESCM..loading_dates SET LAST_LOAD_DATE='"..c_year.."-"..c_month.."-"..c_day.."';")

end
/

EXECUTE SCRIPT &STAGESCM..LOAD_GAS_PRICE_DATA WITH OUTPUT;

-- working with opening time column 

import into &STAGESCM..PRICES from csv at 'https://dev.azure.com/tankerkoenig/362e70d1-bafa-4cf7-a346-1f3613304973/_apis/git/repositories/0d6e7286-91e4-402c-af56-fa75be1f223d/Items?path=/prices/2019/11/' file '2019-11-12-prices.csv' (1 FORMAT='YYYY-MM-DD HH:MI:SS',2..8) COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' row separator = 'LF' skip = 1 REJECT LIMIT 0;

create or replace python scalar script &STAGESCM..json_parsing_time_periods(OPENINGTIMES_JSON VARCHAR(2000000)) 
emits (applicable_days int,start_time varchar(10),end_time varchar(10)) as
import json
def run(ctx):
    opening_times = ctx.OPENINGTIMES_JSON
    if len(opening_times) != 0:
      js = json.loads(opening_times)
      if 'openingTimes' in js:
            for x in range(0,len(js['openingTimes'])):
                    applicable_days = js['openingTimes'][x]['applicable_days']
                    start_time = js['openingTimes'][x]['periods'][0]['startp']
                    end_time = js['openingTimes'][x]['periods'][0]['endp']                
                    ctx.emit(applicable_days,start_time,end_time)
/


create or replace python scalar script &STAGESCM..json_parsing_closing_periods(CLOSINGTIMES_JSON VARCHAR(2000000)) 
emits (start_time varchar(30),end_time varchar(30)) as
import json
def run(ctx):
    closing_times = ctx.CLOSINGTIMES_JSON
    if len(closing_times) != 0:
      js = json.loads(closing_times)
      if 'overrides' in js:
        for y in range(0,len(js['overrides'])):
            start_time = js['overrides'][y]['startp']
            end_time = js['overrides'][y]['endp']                
            ctx.emit(start_time,end_time)
/
