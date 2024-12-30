//Queries to check the result
//staging
/*
truncate table etl_database.etl_schema.staging_weather_data_met_eireann;
truncate table etl_database.etl_schema.staging_passenger_numbers_dublin_bikes;
truncate table staging_Cycle_Counts;

//final
truncate table etl_database.etl_schema.final_table_passenger_numbers_and_dublin_bikes;
truncate table etl_database.etl_schema.final_table_weather_data_met_eireann;
truncate table final_table_cycle_counter_totem;
truncate table final_table_cycle_counter_eco;

//select 

SElect * from etl_database.etl_schema.staging_passenger_numbers_dublin_bikes; //add data to column name
SELECT * FROM etl_database.etl_schema.final_table_passenger_numbers_and_dublin_bikes;


SELECt * FROM staging_weather_data_met_eireann;
SELECT * FROm final_table_weather_data_met_eireann;

SELeCT * FRoM staging_cycle_counts;
sELECT * FROM final_table_cycle_counter_totem;
sELECT * FROM final_table_cycle_counter_eco;

*/
  
//staging for CSV to load Weather_Data_Met_Eireann FROM S3

CREATE OR REPLACE STAGE s3_csv_weather_stage_snowflake
  URL = 's3://s3carloslinaresk81/s3_bucket_folder/'
  CREDENTIALS = (
    AWS_KEY_ID = 'xxxxxx'
    AWS_SECRET_KEY = 'xxxxx'
  )
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 14);

  //create staging for loading bicible count FROM S3

  CREATE OR REPLACE STAGE s3_csv_Cycle_Counts
  URL = 's3://s3carloslinaresk81/s3_excel_bucket_folder/'
  CREDENTIALS = (
    AWS_KEY_ID = 'xxxxxxx'
    AWS_SECRET_KEY = 'xxxxxx'
  )
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

  


