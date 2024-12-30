import json
import requests
import logging
import snowflake.connector
from io import StringIO
from io import BytesIO
from datetime import datetime
import boto3
import pandas as pd
import sys
print(sys.executable)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
################################################################################################################Configuration Section######################################################################################
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    return logging.getLogger(__name__)

# Snowflake connection configuration (replace with actual credentials)
SNOWFLAKE_CONFIG = {
    "user": "xxxxxx",
    "password": "xxxxx",
    "account": "xxxx",
    "warehouse": "COMPUTE_WH",
    "database": "ETL_DATABASE",
    "schema": "ETL_SCHEMA",
    "role": "ACCOUNTADMIN"
}

def create_snowflake_connection():
    """Create a connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            password=SNOWFLAKE_CONFIG["password"],
            account=SNOWFLAKE_CONFIG["account"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"]
        )
        logging.info("Snowflake connection established.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise


# Table names to load data into snowflake staging and final tables
STAGING_TABLE = "staging_Passenger_Numbers_dublin_bikes"
FINAL_TABLE = "final_table_Passenger_Numbers_and_dublin_bikes"
STAGING_TABLE_EXCEL = "staging_Cycle_Counts"
FINAL_TABLE_EXCEL_TOTEM = "final_table_Cycle_Counter_Totem"
FINAL_TABLE_EXCEL_ECO = "final_table_Cycle_Counter_Eco"
CSV_STAGING_TABLE = "staging_Weather_Data_Met_Eireann"
CSV_FINAL_TABLE = "final_table_Weather_Data_Met_Eireann"



# URLS files from API to be extracted
JSON_URLS = {
    "Luas_Passenger_Numbers": 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA11/JSON-stat/1.0/en',
    "Dublin_Bus_Passenger_Numbers": 'https://ws.cso.ie/public/api.jsonrpc?data=%7B%22jsonrpc%22:%222.0%22,%22method%22:%22PxStat.Data.Cube_API.ReadDataset%22,%22params%22:%7B%22class%22:%22query%22,%22id%22:%5B%5D,%22dimension%22:%7B%7D,%22extension%22:%7B%22pivot%22:null,%22codes%22:false,%22language%22:%7B%22code%22:%22en%22%7D,%22format%22:%7B%22type%22:%22JSON-stat%22,%22version%22:%222.0%22%7D,%22matrix%22:%22TOA14%22%7D,%22version%22:%222.0%22%7D%7D',
    "Dublin_Bikes": 'https://data.smartdublin.ie/dublinbikes-api/bikes/openapi.json'
}
#CSV URL
CSV_URL = {
"Weather_Data_Met_Eireann": 'https://cli.fusio.net/cli/climate_data/webdata/dly3923.csv'
}
#Excel URL for data source to be extracted
EXCEL_URL = {
    "Cycle_Counts": 'https://data.smartdublin.ie/dataset/d26ce6c0-2e1c-4b72-8fbd-cb9f9cbbc118/resource/d5c64b9a-27af-411f-81b1-35b3add5a279/download/cycle-counts-1-jan-31-december-2023.xlsx'
}

# S3 configuration:
# the Excel extract needs this fuction and it complements the excel being loaded to S3, so this is included this as part of the S3 configuration although not required to access S3 as well as formatted_datetime and current_date time
def load_excel_from_url(excel_url):
    """Load CSV data from a URL, skipping lines before the header."""
    try:
        response = requests.get(excel_url)
        if response.status_code == 200:
            # Read the Excel file into a pandas DataFrame
            excel_data = BytesIO(response.content)
            df = pd.read_excel(excel_data)
            # Save it as a CSV file in memory
            csv_data = df.to_csv(index=False)
            return csv_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CSV from {excel_url}: {e}")
        raise

    # Get the current date and time and this is use for naming the files uploaded to S3
current_datetime = datetime.now()

    # Format the current date and time as a string (e.g., '2024-12-27_15-30-00')
formatted_datetime = current_datetime.strftime('%Y_%m_%d_%H_%M_%S')

aws_access_key = "xxxxxx"
aws_secret_key = "xxxxxxxx"
region = "us-east-2"
file_url = CSV_URL["Weather_Data_Met_Eireann"]
excel_csv_data = load_excel_from_url(EXCEL_URL["Cycle_Counts"])
bucket_name = "s3carloslinaresk81"
s3_file_name = f"Weather_Data_Met_Eireann_{formatted_datetime}.csv"
s3_excel_file_name = f"Cycle_Counts_{formatted_datetime}.csv"
folder_name = "s3_bucket_folder"
s3_excel_folder_name = "s3_excel_bucket_folder"
s3_folder_file_name = f"{folder_name}/{s3_file_name}"
s3_excel_folder_file_name = f"{s3_excel_folder_name}/{s3_excel_file_name}"

#create tables required in snowflake for data to be loaded into
def create_table_if_not_exists(cursor, table_name, create_query):
    try:
        cursor.execute(create_query)
        logging.info(f"Table '{table_name}' created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {e}")
        raise

#############################################################################################################EXTRACT##################################################################################################
def load_json_from_url(url):
    """Extract JSON data from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error if the request failed
        logging.info(f"Data loaded from URL: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        raise

def load_csv_from_url(url, header_line):
        """Load CSV data from a URL, skipping lines before the header."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            csv_data = response.text.splitlines()
            return csv_data[header_line - 1:]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching CSV from {url}: {e}")
            raise
##########################################################################################################TRANSFORM#######################################################################################################
# this is done for data to be loaded faster instead of using a row by row insert. The copy into snowflake command is faster and that's why it's used later on.
def upload_csv_to_s3(aws_access_key, aws_secret_key, region, file_url, bucket_name, s3_file_name):
    """
    Function to download a CSV from a URL and upload it to Amazon S3.

    :param aws_access_key: AWS Access Key ID
    :param aws_secret_key: AWS Secret Access Key
    :param region: AWS region (e.g., 'us-west-2')
    :param file_url: URL of the CSV file to download
    :param bucket_name: Name of the target S3 bucket
    :param s3_file_name: Name to save the file as in the S3 bucket
    """
    # Initialize the boto3 client with the provided AWS credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

    # Download the file from the URL
    response = requests.get(file_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the content to a local file
        local_file_name = s3_file_name  # Local file name to match S3 file name
        with open(local_file_name, 'wb') as file:
            file.write(response.content)


        # Upload the csv file to S3
        s3.upload_file(local_file_name, bucket_name, s3_folder_file_name)
        print(f"File '{s3_file_name}' uploaded to S3 bucket '{bucket_name}/{s3_folder_file_name}'")

    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

upload_csv_to_s3(aws_access_key, aws_secret_key, region, file_url, bucket_name, s3_file_name)

def upload_converted_excel_to_csv_to_s3(aws_access_key, aws_secret_key, region, excel_csv_data, bucket_name, s3_excel_file_name):
    """
    Function to download a excel converted to CSV  and upload it to Amazon S3. This needs to be done because snowflake is optimize to work with csv and not with xlsx excel files


    :param aws_access_key: AWS Access Key ID
    :param aws_secret_key: AWS Secret Access Key
    :param region: AWS region (e.g., 'us-west-2')
    :param file_url: URL of the CSV file to download
    :param bucket_name: Name of the target S3 bucket
    :param s3_file_name: Name to save the file as in the S3 bucket
    """
    # Initialize the boto3 client with the provided AWS credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

## create the coverted excel to csv file to be uploaded to S3
    local_file_name_two = s3_excel_file_name
    with open(local_file_name_two, 'wb') as file:
        excel_csv_to_bytes = excel_csv_data.encode('utf-8')
        file.write(excel_csv_to_bytes)
    s3.upload_file(local_file_name_two, bucket_name, s3_excel_folder_file_name)
    print(f"File '{s3_excel_file_name}' uploaded to S3 bucket '{bucket_name}/{s3_excel_folder_file_name}'")

# Call the function to upload the file
upload_converted_excel_to_csv_to_s3(aws_access_key, aws_secret_key, region, excel_csv_data, bucket_name, s3_excel_file_name)

def insert_json_data_to_staging(cursor, json_data, source_url):
    """Insert JSON data into the staging table."""
    insert_query = f"""
        INSERT INTO {STAGING_TABLE} (json_column, source_url)
        SELECT PARSE_JSON(%s), %s
    """
    try:
        cursor.execute(insert_query, (json.dumps(json_data), source_url))
        logging.info(f"Data from {source_url} inserted into the staging table.")
    except Exception as e:
        logging.error(f"Error inserting data into staging table: {e}")
        raise

def insert_csv_data_to_staging(cursor):
    """Insert CSV data into the Snowflake table."""
    insert_query = f"""
            COPY INTO {CSV_STAGING_TABLE}(
            sample_date,
            indicator,
            rain,
            indicator_maximum_Temp_Celsius,
            maximum_Temp_Celsius,
            indicator_Minimum_Temp_Celsius,
            Minimum_Temp_Celsius,
            nine_utc_Grass_Minimum_Temperature,
            ten_cm_Soil_Temp
                            )
            FROM @s3_csv_weather_stage_snowflake
            FILES = ('{s3_file_name}')
            ON_ERROR = 'CONTINUE';
           """
    logging.info("Copy csv into staging done.")
    try:
        cursor.execute(insert_query)
    except Exception as e:
        logging.error(f"Error loading CSV data into staging: {e}")
        raise

def insert_csv_excel_data_to_staging(cursor):
        """Insert CSV data into the Snowflake table."""
        insert_query = f"""
                COPY INTO {STAGING_TABLE_EXCEL}(time,
            Charleville_Mall_total,
            Charleville_Mall_North_Cyclist,
            Charleville_Mall_South_Cyclist,
            Clontarf_James_Larkin_Rd_total,
            Clontarf_James_Larkin_Rd_Cyclist_West,
            Clontarf_James_Larkin_Rd_Cyclist_East,
            Clontarf_Pebble_Beach_Carpark_total,
            Clontarf_Pebble_Beach_Carpark_Cyclist_West,
            Clontarf_Pebble_Beach_Carpark_Cyclist_East,
            Drumcondra_Cyclists_Inbound_Cyclist_total,
            Drumcondra_Cyclists_Inbound_Cyclist_West,
            Drumcondra_Cyclists_Inbound_Cyclist_East,
            Drumcondra_Cyclists_Outbound_total,
            Drumcondra_Cyclists_Outbound_Cyclist_East,
            Drumcondra_Cyclists_Outbound_Cyclist_West,
            Griffith_Avenue_Clare_Rd_Side_total,
            Griffith_Avenue_Clare_Rd_Side_Cyclist_South,
            Griffith_Avenue_Clare_Rd_Side_Cyclist_North,
            Griffith_Avenue_Lane_Side_total,
            Griffith_Avenue_Lane_Side_Cyclist_South,
            Griffith_Avenue_Lane_Side_Cyclist_North,
            Grove_Road_Totem_total,
            Grove_Road_Totem_OUT,
            Grove_Road_Totem_IN,
            North_Strand_Rd_N_B_Cyclist,
            North_Strand_Rd_S_B_Cyclist,
            Richmond_Street_Inbound_total,
            Richmond_Street_Inbound_Cyclist_South,
            Richmond_Street_Inbound_Cyclist_North,
            Richmond_Street_Outbound_total,
            Richmond_Street_Outbound_Cyclist_North,
            Richmond_Street_Outbound_Cyclist_South
            )
                FROM @s3_csv_Cycle_Counts
                FILES = ('{s3_excel_file_name}')
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                ON_ERROR = 'CONTINUE';
               """
        logging.info("Copy converted excel to csv into staging done.")
        try:
            cursor.execute(insert_query)
        except Exception as e:
            logging.error(f"Error loading converted excel to CSV data into staging: {e}")
            raise
##################################################################################################LOAD#################################################################################################################
def move_data_to_final(cursor):
    """Move data from the staging table to the final table."""
    #move json to final
    #truncate_query = f"TRUNCATE TABLE {FINAL_TABLE};"

    transfer_query = f"""
        INSERT INTO {FINAL_TABLE} (json_column, source_url, load_date)
        SELECT json_column, source_url, CURRENT_TIMESTAMP()
        FROM {STAGING_TABLE}
        WHERE DATE(load_date) = (SELECT DATE(MAX(load_date)) FROM {STAGING_TABLE});
    """
    #CSV move to final
    #truncate_query_csv = f"TRUNCATE TABLE {CSV_FINAL_TABLE}"

    transfer_query_csv = f"""
            INSERT INTO {CSV_FINAL_TABLE} (sample_date,indicator,rain,indicator_maximum_Temp_Celsius,maximum_Temp_Celsius,indicator_Minimum_Temp_Celsius,Minimum_Temp_Celsius,nine_utc_Grass_Minimum_Temperature,ten_cm_Soil_Temp)
            SELECT TO_VARCHAR(TO_DATE(sample_date, 'DD-MON-YYYY'),'MM/DD/YYYY') as converted_date,indicator,rain,nullif(indicator_maximum_Temp_Celsius,' '),nullif(maximum_Temp_Celsius,' '),nullif(indicator_Minimum_Temp_Celsius,' '),
            nullif(Minimum_Temp_Celsius,' '),nullif(nine_utc_Grass_Minimum_Temperature,' '),nullif(ten_cm_Soil_Temp,' ')
            FROM {CSV_STAGING_TABLE}
            WHERE DATE(load_date) = (SELECT DATE(MAX(load_date)) FROM {CSV_STAGING_TABLE});
            """
    transfer_query_excel_converted_csv_totem = f"""
                INSERT INTO {FINAL_TABLE_EXCEL_TOTEM} (time,Grove_Road_total,Grove_Road_OUT,Grove_Road_IN,load_date)
                SELECT time,nullif(Grove_Road_totem_total,''),nullif(Grove_Road_totem_OUT,''),nullif(Grove_Road_totem_IN,''),load_date
                FROM {STAGING_TABLE_EXCEL}
                WHERE DATE(load_date) = (SELECT DATE(MAX(load_date)) FROM {STAGING_TABLE_EXCEL});
            """
    transfer_query_excel_converted_csv_eco = f"""
                INSERT INTO {FINAL_TABLE_EXCEL_ECO} (time,CHARLEVILLE_MALL_TOTAL,Charleville_Mall_North_Cyclist,Charleville_Mall_South_Cyclist,Clontarf_James_Larkin_Rd_total,Clontarf_James_Larkin_Rd_Cyclist_West,
                Clontarf_James_Larkin_Rd_Cyclist_East,Clontarf_Pebble_Beach_Carpark_total,Clontarf_Pebble_Beach_Carpark_Cyclist_West,Clontarf_Pebble_Beach_Carpark_Cyclist_East,Drumcondra_Cyclists_Inbound_Cyclist_total,
                Drumcondra_Cyclists_Inbound_Cyclist_West,Drumcondra_Cyclists_Inbound_Cyclist_East,Drumcondra_Cyclists_Outbound_total,Drumcondra_Cyclists_Outbound_Cyclist_East,Drumcondra_Cyclists_Outbound_Cyclist_West,
                Griffith_Avenue_Clare_Rd_Side_total,Griffith_Avenue_Clare_Rd_Side_Cyclist_South,Griffith_Avenue_Clare_Rd_Side_Cyclist_North,Griffith_Avenue_Lane_Side_total,Griffith_Avenue_Lane_Side_Cyclist_South,
                Griffith_Avenue_Lane_Side_Cyclist_North,Richmond_Street_Inbound_total,Richmond_Street_Inbound_Cyclist_South,Richmond_Street_Inbound_Cyclist_North,Richmond_Street_Outbound_total,Richmond_Street_Outbound_Cyclist_North,
                Richmond_Street_Outbound_Cyclist_South,load_date) 
                SELECT time,NULLIF(Charleville_Mall_total,''),
                NULLIF(Charleville_Mall_North_Cyclist,''),NULLIF(Charleville_Mall_South_Cyclist,''),NULLIF(Clontarf_James_Larkin_Rd_total,''),NULLIF(Clontarf_James_Larkin_Rd_Cyclist_West,''),NULLIF(Clontarf_James_Larkin_Rd_Cyclist_East,''),
                NULLIF(Clontarf_Pebble_Beach_Carpark_total,''),NULLIF(Clontarf_Pebble_Beach_Carpark_Cyclist_West,''),NULLIF(Clontarf_Pebble_Beach_Carpark_Cyclist_East,''),NULLIF(Drumcondra_Cyclists_Inbound_Cyclist_total,''),
                NULLIF(Drumcondra_Cyclists_Inbound_Cyclist_West,''),NULLIF(Drumcondra_Cyclists_Inbound_Cyclist_East,''),NULLIF(Drumcondra_Cyclists_Outbound_total,''),
                NULLIF(Drumcondra_Cyclists_Outbound_Cyclist_East,''),NULLIF(Drumcondra_Cyclists_Outbound_Cyclist_West,''),NULLIF(Griffith_Avenue_Clare_Rd_Side_total,''),NULLIF(Griffith_Avenue_Clare_Rd_Side_Cyclist_South,''),
                NULLIF(Griffith_Avenue_Clare_Rd_Side_Cyclist_North,''),NULLIF(Griffith_Avenue_Lane_Side_total,''),NULLIF(Griffith_Avenue_Lane_Side_Cyclist_South,''),NULLIF(Griffith_Avenue_Lane_Side_Cyclist_North,''),
                NULLIF(Richmond_Street_Inbound_total,''),NULLIF(Richmond_Street_Inbound_Cyclist_South,''),NULLIF(Richmond_Street_Inbound_Cyclist_North,''),NULLIF(Richmond_Street_Outbound_total,''),
                NULLIF(Richmond_Street_Outbound_Cyclist_North,''),NULLIF(Richmond_Street_Outbound_Cyclist_South,''),load_date
                FROM {STAGING_TABLE_EXCEL}
                WHERE DATE(load_date) = (SELECT DATE(MAX(load_date)) FROM {STAGING_TABLE_EXCEL});
                """
    # run the queries defined for the load in snowflake
    try:
        #cursor.execute(truncate_query)
        cursor.execute(transfer_query)
        logging.info("json Data moved from staging to final table.")
        #cursor.execute(truncate_query_csv)
        cursor.execute(transfer_query_csv)
        logging.info("CSV Data moved from staging to final table.")
        cursor.execute(transfer_query_excel_converted_csv_totem)
        cursor.execute(transfer_query_excel_converted_csv_eco)
    except Exception as e:
        logging.error(f"Error moving CSV of json data to final table: {e}")
        raise


    # --------------------------
    # Main ETL Process
    # --------------------------

def main():
    """Main ETL Process: Extract, Transform, Load"""
    # Setup logging
    logger = setup_logging()

    # Extract JSON data from URLs
    try:
        json_data1 = load_json_from_url(JSON_URLS["Luas_Passenger_Numbers"])
        json_data2 = load_json_from_url(JSON_URLS["Dublin_Bus_Passenger_Numbers"])
        json_data3 = load_json_from_url(JSON_URLS["Dublin_Bikes"])
    except Exception as e:
        logger.error("Error during extraction. Exiting ETL process.")
        return

    # Extract CSV data from URLs
    try:
        csv_data = load_csv_from_url(CSV_URL["Weather_Data_Met_Eireann"], header_line=14)
    except Exception as e:
        logger.error("Error during CSV extraction. Exiting process.")
        return
    try:
        conn = create_snowflake_connection()
        cursor = conn.cursor()

        # Create staging and final tables if they don't exist for json files
        create_staging_query = f"""
            CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
                json_column VARIANT,
                source_url STRING,
                load_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """
        create_final_query = f"""
            CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
                json_column VARIANT,
                source_url STRING,
                load_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """

        create_csv_staging_query = f"""
                CREATE TABLE IF NOT EXISTS {CSV_STAGING_TABLE} (
                    sample_date STRING,
                    indicator tinyint,
                    rain decimal(3,1),
                    indicator_maximum_Temp_Celsius string,
                    maximum_Temp_Celsius string,
                    indicator_Minimum_Temp_Celsius string,
                    Minimum_Temp_Celsius string,
                    nine_utc_Grass_Minimum_Temperature string,
                    ten_cm_Soil_Temp string,
                    load_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                );
            """

        create_csv_final_query = f"""
                CREATE TABLE IF NOT EXISTS {CSV_FINAL_TABLE} (
                    sample_date date,
                    indicator tinyint,
                    rain decimal(3,1),
                    indicator_maximum_Temp_Celsius tinyint,
                    maximum_Temp_Celsius decimal(3,1),
                    indicator_Minimum_Temp_Celsius tinyint,
                    Minimum_Temp_Celsius decimal(3,1),
                    nine_utc_Grass_Minimum_Temperature FLOAT,
                    ten_cm_Soil_Temp FLOAT,
                    load_date TIMESTAMP_LTZ
                );
            """
        create_staging_query_excel = f"""
                CREATE TABLE IF NOT EXISTS {STAGING_TABLE_EXCEL} (
                time TIMESTAMP,
                Charleville_Mall_total STRING,
                Charleville_Mall_North_Cyclist STRING,
                Charleville_Mall_South_Cyclist STRING,
                Clontarf_James_Larkin_Rd_total STRING,
                Clontarf_James_Larkin_Rd_Cyclist_West STRING,
                Clontarf_James_Larkin_Rd_Cyclist_East STRING,
                Clontarf_Pebble_Beach_Carpark_total STRING,
                Clontarf_Pebble_Beach_Carpark_Cyclist_West STRING,
                Clontarf_Pebble_Beach_Carpark_Cyclist_East STRING,
                Drumcondra_Cyclists_Inbound_Cyclist_total STRING,
                Drumcondra_Cyclists_Inbound_Cyclist_West STRING,
                Drumcondra_Cyclists_Inbound_Cyclist_East STRING,
                Drumcondra_Cyclists_Outbound_total STRING,
                Drumcondra_Cyclists_Outbound_Cyclist_East STRING,
                Drumcondra_Cyclists_Outbound_Cyclist_West STRING,
                Griffith_Avenue_Clare_Rd_Side_total STRING,
                Griffith_Avenue_Clare_Rd_Side_Cyclist_South STRING,
                Griffith_Avenue_Clare_Rd_Side_Cyclist_North STRING,
                Griffith_Avenue_Lane_Side_total STRING,
                Griffith_Avenue_Lane_Side_Cyclist_South STRING,
                Griffith_Avenue_Lane_Side_Cyclist_North STRING,
                Grove_Road_Totem_total STRING,
                Grove_Road_Totem_OUT STRING,
                Grove_Road_Totem_IN STRING,
                North_Strand_Rd_N_B_Cyclist STRING,
                North_Strand_Rd_S_B_Cyclist STRING,
                Richmond_Street_Inbound_total STRING,
                Richmond_Street_Inbound_Cyclist_South STRING,
                Richmond_Street_Inbound_Cyclist_North STRING,
                Richmond_Street_Outbound_total STRING,
                Richmond_Street_Outbound_Cyclist_North STRING,
                Richmond_Street_Outbound_Cyclist_South STRING,
                load_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                );
            """
        #this table will only have column w,x,y of the excel
        create_final_query_excel_totem = f"""
            CREATE TABLE IF NOT EXISTS {FINAL_TABLE_EXCEL_TOTEM} (
                time TIMESTAMP,
                Grove_Road_total INT,
                Grove_Road_OUT INT,
                Grove_Road_IN INT,
                load_date TIMESTAMP_LTZ
            );
        """
        #this table will only have all columns except WXY
        create_final_query_excel_eco = f"""
                    CREATE TABLE IF NOT EXISTS {FINAL_TABLE_EXCEL_ECO} (
                    time timestamp,
                    Charleville_Mall_total INT,
                    Charleville_Mall_North_Cyclist INT,
                    Charleville_Mall_South_Cyclist INT,
                    Clontarf_James_Larkin_Rd_total INT,
                    Clontarf_James_Larkin_Rd_Cyclist_West INT,
                    Clontarf_James_Larkin_Rd_Cyclist_East INT,
                    Clontarf_Pebble_Beach_Carpark_total INT,
                    Clontarf_Pebble_Beach_Carpark_Cyclist_West INT,
                    Clontarf_Pebble_Beach_Carpark_Cyclist_East INT,
                    Drumcondra_Cyclists_Inbound_Cyclist_total INT,
                    Drumcondra_Cyclists_Inbound_Cyclist_West INT,
                    Drumcondra_Cyclists_Inbound_Cyclist_East INT,
                    Drumcondra_Cyclists_Outbound_total INT,
                    Drumcondra_Cyclists_Outbound_Cyclist_East INT,
                    Drumcondra_Cyclists_Outbound_Cyclist_West INT,
                    Griffith_Avenue_Clare_Rd_Side_total INT,
                    Griffith_Avenue_Clare_Rd_Side_Cyclist_South INT,
                    Griffith_Avenue_Clare_Rd_Side_Cyclist_North INT,
                    Griffith_Avenue_Lane_Side_total INT,
                    Griffith_Avenue_Lane_Side_Cyclist_South INT,
                    Griffith_Avenue_Lane_Side_Cyclist_North INT,
                    Richmond_Street_Inbound_total INT,
                    Richmond_Street_Inbound_Cyclist_South INT,
                    Richmond_Street_Inbound_Cyclist_North INT,
                    Richmond_Street_Outbound_total INT,
                    Richmond_Street_Outbound_Cyclist_North INT,
                    Richmond_Street_Outbound_Cyclist_South INT,
                    load_date TIMESTAMP_LTZ
                    );
                """


        create_table_if_not_exists(cursor, STAGING_TABLE, create_staging_query)
        create_table_if_not_exists(cursor, FINAL_TABLE, create_final_query)
        create_table_if_not_exists(cursor, CSV_STAGING_TABLE, create_csv_staging_query)
        create_table_if_not_exists(cursor, CSV_FINAL_TABLE, create_csv_final_query)
        create_table_if_not_exists(cursor, STAGING_TABLE_EXCEL, create_staging_query_excel)
        create_table_if_not_exists(cursor,FINAL_TABLE_EXCEL_TOTEM, create_final_query_excel_totem)
        create_table_if_not_exists(cursor,FINAL_TABLE_EXCEL_ECO, create_final_query_excel_eco)

        # Insert extracted data into staging table
        insert_json_data_to_staging(cursor, json_data1, "Luas_Passenger_Numbers")
        insert_json_data_to_staging(cursor, json_data2, "Dublin_Bus_Passenger_Numbers")
        insert_json_data_to_staging(cursor, json_data3, "Dublin_Bikes")
        insert_csv_data_to_staging(cursor)
        insert_csv_excel_data_to_staging(cursor)#here it calls the insert

        # Move data from staging to final table
        move_data_to_final(cursor)

        # Commit changes
        conn.commit()
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
    finally:
        # Cleanup
        if conn:
            cursor.close()
            conn.close()
            logging.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()