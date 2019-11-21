# German gas prices

Example to load [Tankerkoenig - current fuel prices](https://tankerkoenig.de/) in an Exasol database. 

### Prerequisites: 

- EXASOL database that is allowed to connect to the Internet and that has access to a nameserver

### Importing the dataset

Run the SQL files in the following order 

- 01_setup_schema.sql
- 02_create_tables.sql
- 03_create_scripts.sql
- 04_load_data.sql

Scripts are implemented in a way, that a delta load should be easily possible in case new data gets published (as long as the format stays the same). Furthermore the load-script can be resumed (reloading the last/current file) in case a filetransfer got interrupted). 

## Related Material 

Tankerkoenig resources: 

- Website: https://creativecommons.tankerkoenig.de/
- Repository for historical data:  https://dev.azure.com/tankerkoenig/_git/tankerkoenig-data?path=%2F&version=GBmaster&_a=history

