-- script to parse opening time column to fetch opening times from stations table 
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

-- script to parse opening time column to fetch closing times from stations table
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

-- load script
-- script to auto load data from repository
-- currently set to load data from previous day
CREATE OR REPLACE LUA SCRIPT &STAGESCM..LOAD_CURRENT_PRICE_DATA(c_year,c_month,c_day) RETURNS TABLE AS
import('GAS_PRICES_STAGE.QUERY_WRAPPER','QW')

-- add trailing zero to month and day if length is one
if string.len(c_month) < 2 then
	c_month = "0"..c_month
end
if string.len(c_day) < 2 then
	c_day = "0"..c_day 
end

--initialize query wrapper
wrapper = QW.new( '&STAGESCM..JOB_LOG', '&STAGESCM..JOB_DETAILS', 'LOAD_CURRENT_PRICE_DATA')

--set SCHEMA / TABLE for staging tables
wrapper:set_param('STAGE_SCM',quote('&STAGESCM')) 
wrapper:set_param('PROD_SCM',quote('&PRODSCM')) 

-- ****** SUBSTANCES *********

wrapper:set_param('C_YEAR',c_year)
wrapper:set_param('C_MONTH',c_month)
wrapper:set_param('C_DAY',c_day)
wrapper:set_param('SKIP_ROWS',1)
wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',5)
wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRACTICAL_ADDRESS'))
wrapper:set_param('SITE_URL',[[https://dev.azure.com/tankerkoenig/362e70d1-bafa-4cf7-a346-1f3613304973/_apis/git/repositories/0d6e7286-91e4-402c-af56-fa75be1f223d/Items?path=]])

----***STATIONS***-----
wrapper:set_param('SITE_URL_STATIONS',wrapper:get_param('SITE_URL')..[[/stations/]]..wrapper:get_param('C_YEAR')..[[/]]..wrapper:get_param('C_MONTH')..[[/]])
wrapper:set_param('FILE_NAME_STATIONS',wrapper:get_param('C_YEAR')..[[-]]..wrapper:get_param('C_MONTH')..[[-]]..wrapper:get_param('C_DAY')..[[-stations.csv]])

-- delete all previous data from stations staging table
wrapper:query([[truncate table ::STAGE_SCM.STATIONS]])

--import new data in stations staging table

_,res = wrapper:query([[import into ::STAGE_SCM.STATIONS FROM CSV AT :SITE_URL_STATIONS FILE :FILE_NAME_STATIONS COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' row separator='LF' TRIM SKIP=]]..wrapper:get_param('SKIP_ROWS')..[[ ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE;]])
if (res.etl_rows_with_error > 0) then
		wrapper:log('WARN','FILE found rows with errors',res.etl_rows_with_error)
end	

-- strip first_active column to match timestamp format
wrapper:query([[UPDATE ::STAGE_SCM.STATIONS SET FIRST_ACTIVE=SUBSTRING(FIRST_ACTIVE,0,19);]])

-- merge into stations production table
wrapper:query([[merge into ::PROD_SCM.STATIONS tgt using ::STAGE_SCM.STATIONS src on TGT.UUID = src.UUID when not matched then insert (UUID,NAME,BRAND,STREET,HOUSE_NUMBER,POST_CODE,CITY,LATITUDE,LONGITUDE,FIRST_ACTIVE) values (UUID,NAME,BRAND,STREET,HOUSE_NUMBER,POST_CODE,CITY,LATITUDE,LONGITUDE,FIRST_ACTIVE);]])

-- convert first_active column values from UTC timezone to Berlin timezone
wrapper:query([[UPDATE ::PROD_SCM.STATIONS SET FIRST_ACTIVE = CONVERT_TZ(FIRST_ACTIVE,'UTC','Europe/Berlin');]]) 

-- solve for opening and closing times ---
-- insert all new opening times in opening_periods prod table
wrapper:query([[insert into ::PROD_SCM.OPENING_PERIODS(UUID,APPLICABLE_DAYS,START_TIME,END_TIME) (with tmp as (SELECT UUID,OPENINGTIMES_JSON FROM ::STAGE_SCM.STATIONS WHERE UUID NOT IN (SELECT UUID FROM ::PROD_SCM.STATIONS)) SELECT UUID,::STAGE_SCM.json_parsing_time_periods(OPENINGTIMES_JSON) FROM TMP);]])

-- insert all new closing times in closing_periods prod table
wrapper:query([[insert into ::PROD_SCM.CLOSING_PERIODS(UUID,START_TIME,END_TIME) (with tmp as(SELECT UUID,OPENINGTIMES_JSON FROM ::STAGE_SCM.STATIONS WHERE UUID NOT IN (SELECT UUID FROM ::PROD_SCM.STATIONS))SELECT UUID,::STAGE_SCM.json_parsing_closing_periods(OPENINGTIMES_JSON) FROM TMP);]])


----***PRICES***-----
wrapper:set_param('SITE_URL_PRICES',wrapper:get_param('SITE_URL')..[[/prices/]]..wrapper:get_param('C_YEAR')..[[/]]..wrapper:get_param('C_MONTH')..[[/]])
wrapper:set_param('FILE_NAME_PRICES',wrapper:get_param('C_YEAR')..[[-]]..wrapper:get_param('C_MONTH')..[[-]]..wrapper:get_param('C_DAY')..[[-prices.csv]])

-- delete all previous data from pricing staging table
wrapper:query([[truncate table ::STAGE_SCM.PRICES]])

-- insert check making sure data is not added twice
_,restab = wrapper:query([[SELECT last_load_date from &STAGESCM..loading_dates]])
if ("'"..c_year.."-"..c_month.."-"..c_day.."'" ~= "'"..tostring(restab[1][1]).."'") then 
	
	-- import new data in pricing staging table
    _,res = wrapper:query([[import into ::STAGE_SCM.PRICES FROM CSV AT :SITE_URL_PRICES FILE :FILE_NAME_PRICES COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' row separator='LF' TRIM SKIP=]]..wrapper:get_param('SKIP_ROWS')..[[ ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE;]])
	if (res.etl_rows_with_error > 0) then
		wrapper:log('WARN','FILE found rows with errors',res.etl_rows_with_error)
	end	

	-- strip MODIFICATION_TIME column to match timestamp format
	wrapper:query([[UPDATE ::STAGE_SCM.PRICES SET MODIFICATION_TIME=SUBSTRING(MODIFICATION_TIME,0,19);]])
	
	-- insert into production table 
	wrapper:query([[insert into ::PROD_SCM.PRICES(MODIFICATION_TIME, UUID, DIESEL, E5, E10, DIESEL_CHANGE, E5_CHANGE, E10_CHANGE) select MODIFICATION_TIME, UUID, DIESEL, E5, E10, DIESEL_CHANGE, E5_CHANGE, E10_CHANGE from ::STAGE_SCM.PRICES WHERE UUID IN (SELECT UUID FROM ::PROD_SCM.STATIONS);]])
	
	-- convert MODIFICATION_TIME column values from UTC timezone to Berlin timezone
	wrapper:query([[UPDATE ::PROD_SCM.PRICES SET MODIFICATION_TIME = CONVERT_TZ(MODIFICATION_TIME,'UTC','Europe/Berlin');]]) 

	-- update loading_dates 
	wrapper:query([[UPDATE &STAGESCM..loading_dates SET LAST_LOAD_DATE=']]..wrapper:get_param('C_YEAR')..[[-]]..wrapper:get_param('C_MONTH')..[[-]]..wrapper:get_param('C_DAY')..[[';]])
end

return wrapper:finish()
/

-- script to update data, need to execute once
CREATE OR REPLACE SCRIPT &STAGESCM..UPDATE_DATA AS
local date_table = os.date("*t")
local c_year,c_month,c_day = date_table.year, date_table.month, date_table.day-1
query("EXECUTE SCRIPT &STAGESCM..LOAD_CURRENT_PRICE_DATA("..c_year..","..c_month..","..c_day..") WITH OUTPUT;")
/

-- delta load script
-- run it once to load all available gas price data
CREATE OR REPLACE SCRIPT &STAGESCM..LOAD_ALL_HISTORIC_DATA(min_year,max_year,min_month,max_month,min_day,max_day) AS
c_year = min_year
for c_year = min_year,max_year do
	for c_month = min_month,max_month do
		for c_day = min_day,max_day do
		query("EXECUTE SCRIPT &STAGESCM..LOAD_GAS_PRICE_DATA("..c_year..","..c_month..","..c_day..") WITH OUTPUT")
		end
	end
end 
/

