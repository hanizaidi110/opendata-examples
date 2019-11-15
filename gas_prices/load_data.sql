-- script to update data, need to execute once

CREATE OR REPLACE SCRIPT &STAGESCM..UPDATE_DATA AS
local date_table = os.date("*t")
local c_year,c_month,c_day = date_table.year, date_table.month, date_table.day-1
query("EXECUTE SCRIPT &STAGESCM..LOAD_GAS_PRICE_DATA("..c_year..","..c_month..","..c_day..") WITH OUTPUT;")
/

-- run this statement daily 
EXECUTE SCRIPT &STAGESCM..UPDATE_DATA;

-- load all previous data from and to given year,month,day
EXECUTE SCRIPT &STAGESCM..LOAD_ALL_HISTORIC_DATA(
min_year = 2014     -- start import from year
,max_year = 2014    -- end import to year
,min_month = 11     -- start import from month
,max_month = 12     -- end import to month
,min_day   = 1      -- start import from day
,max_day   = 31     -- end import to day
);