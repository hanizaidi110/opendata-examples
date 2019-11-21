create or replace table
	&STAGESCM..STATIONS(
		UUID VARCHAR(36) NOT NULL
		,NAME VARCHAR(200)
		,BRAND VARCHAR(100)
		,STREET VARCHAR(200)
		,HOUSE_NUMBER VARCHAR(100)
		,POST_CODE VARCHAR(10)
		,CITY VARCHAR(100)
		,LATITUDE VARCHAR(100)
		,LONGITUDE VARCHAR(100)
		,FIRST_ACTIVE VARCHAR(100)
		,OPENINGTIMES_JSON VARCHAR(2000000)
	); 

create or replace table
	&STAGESCM..PRICES(
		MODIFICATION_TIME VARCHAR(200)
		,UUID VARCHAR(36)
		,DIESEL DECIMAL(5,4)
		,E5 DECIMAL(5,4)
		,E10 DECIMAL(5,4)
		,DIESEL_CHANGE CHAR(1)
		,E5_CHANGE CHAR(1)
		,E10_CHANGE CHAR(1)
	);

create or replace table
	&PRODSCM..STATIONS(
		UUID VARCHAR(36) NOT NULL
		,NAME VARCHAR(200)
		,BRAND VARCHAR(100)
		,STREET VARCHAR(200)
		,HOUSE_NUMBER VARCHAR(100)
		,POST_CODE VARCHAR(10)
		,CITY VARCHAR(100)
		,LATITUDE VARCHAR(100)
		,LONGITUDE VARCHAR(100)
		,FIRST_ACTIVE TIMESTAMP
		,PRIMARY KEY(UUID) 
	); 

create or replace table
	&PRODSCM..PRICES(
		MODIFICATION_TIME TIMESTAMP
		,UUID VARCHAR(36)
		,DIESEL DECIMAL(5,4)
		,E5 DECIMAL(5,4)
		,E10 DECIMAL(5,4)
		,DIESEL_CHANGE CHAR(1)
		,E5_CHANGE CHAR(1)
		,E10_CHANGE CHAR(1)
		,FOREIGN KEY(UUID) REFERENCES &PRODSCM..STATIONS(UUID) 
);

CREATE OR REPLACE TABLE &PRODSCM..OPENING_PERIODS(
	UUID VARCHAR(36) NOT NULL
	,applicable_days int
	,start_time varchar(10)
	,end_time varchar(10)
	,FOREIGN KEY (UUID) REFERENCES &PRODSCM..STATIONS(UUID)
);

CREATE OR REPLACE TABLE &PRODSCM..CLOSING_PERIODS(
	UUID VARCHAR(36) NOT NULL
	,start_time TIMESTAMP
	,end_time TIMESTAMP
	,FOREIGN KEY (UUID) REFERENCES &PRODSCM..STATIONS(UUID)
);

CREATE OR REPLACE TABLE &STAGESCM..loading_dates(
	last_load_date varchar(10)
);

-- insert a dummy value as first loading_date
INSERT INTO &STAGESCM..loading_dates(last_load_date) VALUES('1970-01-01'); 

--set distribution keys for local joins (reduced network communication)
--ALTER TABLE &PRODSCM..STATIONS DISTRIBUTE BY UUID; not needed  value under 100000 rows (considered small in replication border according to documentation)
ALTER TABLE &PRODSCM..PRICES DISTRIBUTE BY UUID;

/** Query Wrapper: DDL for logging tables **/
CREATE TABLE IF NOT EXISTS &STAGESCM..job_log(
    run_id       INT IDENTITY NOT NULL PRIMARY KEY
  , script_name  VARCHAR(100) NOT NULL
  , status       VARCHAR(100)
  , start_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  , end_time     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS &STAGESCM..job_details (
    detail_id    INT IDENTITY NOT NULL
  , run_id       INT NOT NULL REFERENCES &STAGESCM..job_log ( run_id )
  , log_time     TIMESTAMP
  , log_level    VARCHAR(10)
  , log_message  VARCHAR(20000)
  , rowcount     DECIMAL(18)
);

