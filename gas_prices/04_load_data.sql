-- run this statement daily. Currently set to load data from yesterday
EXECUTE SCRIPT &STAGESCM..UPDATE_DATA;

-- or supply the following parameters to call LOAD_CURRENT_PRICE_DATA script directly
EXECUTE SCRIPT &STAGESCM..LOAD_CURRENT_PRICE_DATA('2019','11','20');

-- load all previous data from and to given year,month,day (delta load)
EXECUTE SCRIPT &STAGESCM..LOAD_ALL_HISTORIC_DATA(
min_year = 2014     -- start import from year
,max_year = 2014    -- end import to year
,min_month = 11     -- start import from month
,max_month = 12     -- end import to month
,min_day   = 1      -- start import from day
,max_day   = 31     -- end import to day
);
