--use variables for the SCHEMA names
define STAGESCM=GAS_PRICES_STAGE;
define PRODSCM=GAS_PRICES;

create schema if not exists &PRODSCM;
create schema if not exists &STAGESCM;





