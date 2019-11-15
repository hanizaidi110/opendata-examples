-- initial load script
-- run it once to load all available gas price data
-- assign the following parameters to select date range to import data 

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

