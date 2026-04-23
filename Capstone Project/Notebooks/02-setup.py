# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

# SetupHelper class handles the complete lifecycle of the health analytics lakehouse:
# - Creating the database and all required tables (bronze, silver, gold layers)
# - Validating the setup by asserting all tables exist
# - Cleaning up resources (database, landing zone, checkpoints)
class SetupHelper():   
    
    # Constructor: Initializes configuration paths and catalog/database settings
    # Parameters:
    #   env (str): The catalog name (e.g., 'dev', 'prod') used as the Unity Catalog namespace
    def __init__(self, env):
        Conf = Config()  # Load configuration from the 01-config notebook (run via %run)
        self.landing_zone = Conf.base_dir_data + "/raw"  # Path for raw data landing zone
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"  # Path for streaming checkpoints
        self.catalog = env  # Unity Catalog name (passed as environment parameter)
        self.db_name = Conf.db_name  # Database/schema name from config
        self.initialized = False  # Flag to track if database has been created
    
    # Creates the database (schema) in the specified catalog
    # Sets the initialized flag to True so table creation methods can proceed
    def create_db(self):
        spark.catalog.clearCache()  # Clear any cached metadata to ensure fresh state
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")  # Create DB if it doesn't exist
        spark.sql(f"USE {self.catalog}.{self.db_name}")  # Set as the active database for subsequent queries
        self.initialized = True  # Mark setup as initialized so tables can be created
        print("Done")
    
    # =====================================================================
    # BRONZE LAYER TABLES (raw ingestion tables, suffix: _bz)
    # These tables store raw data as-is from source systems
    # =====================================================================
    
    # Creates the registered_users_bz table (Bronze layer)
    # Stores raw user registration data with device and MAC address info
    # Includes load_time and source_file for data lineage tracking
    def create_registered_users_bz(self):
        if(self.initialized):  # Only proceed if database was created successfully
            print(f"Creating registered_users_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                    user_id long,                        -- Unique identifier for the user
                    device_id long,                      -- ID of the user's wearable device
                    mac_address string,                  -- MAC address of the device
                    registration_timestamp double,       -- When the user registered (epoch as double)
                    load_time timestamp,                 -- When this record was ingested
                    source_file string                   -- Source file path for lineage tracking
                    )
                  """) 
            print("Done")
        else:
            # Prevent accidental table creation in the default database
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the gym_logins_bz table (Bronze layer)
    # Stores raw gym check-in/check-out data
    # login/logout stored as doubles (epoch timestamps) before silver layer transformation
    def create_gym_logins_bz(self):
        if(self.initialized):
            print(f"Creating gym_logins_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                    mac_address string,                  -- Device MAC address (links to user)
                    gym bigint,                          -- Gym location identifier
                    login double,                        -- Login time as epoch (raw format)
                    logout double,                       -- Logout time as epoch (raw format)
                    load_time timestamp,                 -- Ingestion timestamp
                    source_file string                   -- Source file for lineage
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    # Creates the kafka_multiplex_bz table (Bronze layer)
    # Stores raw Kafka messages from multiple topics in a single multiplexed table
    # Partitioned by topic and week_part for efficient querying and data management
    def create_kafka_multiplex_bz(self):
        if(self.initialized):
            print(f"Creating kafka_multiplex_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                  key string,                            -- Kafka message key
                  value string,                          -- Kafka message value (JSON payload)
                  topic string,                          -- Kafka topic name (e.g., heart_rate, workouts)
                  partition bigint,                       -- Kafka partition number
                  offset bigint,                          -- Kafka offset within the partition
                  timestamp bigint,                       -- Kafka message timestamp
                  date date,                              -- Derived date for partitioning
                  week_part string,                       -- Week partition key (e.g., '2024-W01')
                  load_time timestamp,                    -- Ingestion timestamp
                  source_file string)                     -- Source file for lineage
                  PARTITIONED BY (topic, week_part)       -- Partition by topic and week for performance
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # =====================================================================
    # SILVER LAYER TABLES (cleansed and transformed data)
    # These tables contain validated, deduplicated, and type-corrected data
    # =====================================================================
    
    # Creates the users table (Silver layer)
    # Cleansed version of registered_users_bz with proper timestamp types
    def create_users(self):
        if(self.initialized):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    user_id bigint,                      -- Unique user identifier
                    device_id bigint,                    -- Device identifier
                    mac_address string,                  -- Device MAC address
                    registration_timestamp timestamp     -- Registration time (converted to proper timestamp)
                    )
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the gym_logs table (Silver layer)
    # Cleansed version of gym_logins_bz with login/logout as proper timestamps
    def create_gym_logs(self):
        if(self.initialized):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,                  -- Device MAC address
                    gym bigint,                          -- Gym location ID
                    login timestamp,                     -- Login time (converted from double to timestamp)
                    logout timestamp                     -- Logout time (converted from double to timestamp)
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the user_profile table (Silver layer)
    # Stores demographic and personal information for each user
    # Used for user segmentation and enrichment in gold layer
    def create_user_profile(self):
        if(self.initialized):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    user_id bigint,                      -- Unique user identifier
                    dob DATE,                            -- Date of birth
                    sex STRING,                          -- Biological sex
                    gender STRING,                       -- Gender identity
                    first_name STRING,                   -- First name
                    last_name STRING,                    -- Last name
                    street_address STRING,               -- Street address
                    city STRING,                         -- City
                    state STRING,                        -- State
                    zip INT,                             -- ZIP code
                    updated TIMESTAMP)                   -- Last update timestamp (for CDC tracking)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    # Creates the heart_rate table (Silver layer)
    # Stores validated heart rate readings from wearable devices
    # The 'valid' flag indicates if the reading passed quality checks
    def create_heart_rate(self):
        if(self.initialized):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    device_id LONG,                      -- Device that recorded the heart rate
                    time TIMESTAMP,                      -- Time of the reading
                    heartrate DOUBLE,                    -- Heart rate value (BPM)
                    valid BOOLEAN)                       -- Whether the reading is valid/reliable
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    # Creates the user_bins table (Silver layer)
    # Stores user demographic bins/categories for aggregation
    # Age is stored as a string range (e.g., '25-30') for grouping
    def create_user_bins(self):
        if(self.initialized):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id BIGINT,                      -- Unique user identifier
                    age STRING,                          -- Age range bin (e.g., '25-30')
                    gender STRING,                       -- Gender category
                    city STRING,                         -- City
                    state STRING)                        -- State
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    # Creates the workouts table (Silver layer)
    # Stores individual workout events (start/stop actions) from Kafka stream
    def create_workouts(self):
        if(self.initialized):
            print(f"Creating workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                    user_id INT,                         -- User who performed the workout
                    workout_id INT,                      -- Unique workout identifier
                    time TIMESTAMP,                      -- Time of the workout action
                    action STRING,                       -- Action type ('start' or 'stop')
                    session_id INT)                      -- Session identifier for grouping
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    # Creates the completed_workouts table (Silver layer)
    # Stores paired start/end times for completed workout sessions
    # Derived by matching start and stop actions from the workouts table
    def create_completed_workouts(self):
        if(self.initialized):
            print(f"Creating completed_workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                    user_id INT,                         -- User who completed the workout
                    workout_id INT,                      -- Workout identifier
                    session_id INT,                      -- Session identifier
                    start_time TIMESTAMP,                -- When the workout started
                    end_time TIMESTAMP)                  -- When the workout ended
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the workout_bpm table (Silver layer)
    # Joins heart rate data with completed workouts to get BPM during exercises
    def create_workout_bpm(self):
        if(self.initialized):
            print(f"Creating workout_bpm table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                    user_id INT,                         -- User identifier
                    workout_id INT,                      -- Workout identifier
                    session_id INT,                      -- Session identifier
                    start_time TIMESTAMP,                -- Workout start time
                    end_time TIMESTAMP,                  -- Workout end time
                    time TIMESTAMP,                      -- Heart rate reading time
                    heartrate DOUBLE)                    -- Heart rate value during workout
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the date_lookup table (Silver layer - reference/dimension table)
    # Lookup table for date-based calculations and partitioning
    # Pre-computed date attributes for efficient joins and filtering
    def create_date_lookup(self):
        if(self.initialized):
            print(f"Creating date_lookup table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                    date date,                           -- Calendar date
                    week int,                            -- Week number of the year
                    year int,                            -- Year
                    month int,                           -- Month number
                    dayofweek int,                       -- Day of week (1=Monday, 7=Sunday)
                    dayofmonth int,                      -- Day of the month
                    dayofyear int,                       -- Day of the year
                    week_part string)                    -- Week partition key (matches kafka_multiplex_bz)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # =====================================================================
    # GOLD LAYER TABLES AND VIEWS (business-level aggregations)
    # These are consumption-ready tables/views for analytics and dashboards
    # =====================================================================
    
    # Creates the workout_bpm_summary table (Gold layer)
    # Aggregated BPM statistics per workout session, enriched with user demographics
    # Used for health analytics dashboards and reports
    def create_workout_bpm_summary(self):
        if(self.initialized):
            print(f"Creating workout_bpm_summary table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    workout_id INT,                      -- Workout identifier
                    session_id INT,                      -- Session identifier
                    user_id BIGINT,                      -- User identifier
                    age STRING,                          -- User age bin
                    gender STRING,                       -- User gender
                    city STRING,                         -- User city
                    state STRING,                        -- User state
                    min_bpm DOUBLE,                      -- Minimum BPM during workout
                    avg_bpm DOUBLE,                      -- Average BPM during workout
                    max_bpm DOUBLE,                      -- Maximum BPM during workout
                    num_recordings BIGINT)               -- Number of heart rate readings
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # Creates the gym_summary view (Gold layer - materialized as a VIEW)
    # Joins gym_logs with completed_workouts and users to calculate:
    #   - Total minutes spent in the gym per visit
    #   - Total minutes actually exercising per visit
    # Uses ::timestamp and ::long casting syntax for time calculations
    def create_gym_summary(self):
        if(self.initialized):
            print(f"Creating gym_summar gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login::timestamp) date,       -- Extract date from login timestamp
                            gym, l.mac_address, workout_id, session_id, 
                            -- Calculate total minutes in the gym (logout - login in minutes)
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            -- Calculate total minutes exercising (end_time - start_time in minutes)
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            -- Subquery: Join completed_workouts with users to get MAC address
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            -- Match workouts that started during a gym session
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    # =====================================================================
    # LIFECYCLE METHODS (setup, validate, cleanup)
    # =====================================================================
    
    # Runs the complete setup: creates database and all tables in order
    # Bronze tables first, then silver, then gold
    # Prints total elapsed time upon completion
    def setup(self):
        import time
        start = int(time.time())  # Record start time for duration tracking
        print(f"\nStarting setup ...")
        self.create_db()                    # Step 1: Create the database
        # Bronze layer tables
        self.create_registered_users_bz()   # Step 2: Raw user registrations
        self.create_gym_logins_bz()         # Step 3: Raw gym login/logout
        self.create_kafka_multiplex_bz()    # Step 4: Raw Kafka messages
        # Silver layer tables
        self.create_users()                 # Step 5: Cleansed users
        self.create_gym_logs()              # Step 6: Cleansed gym logs
        self.create_user_profile()          # Step 7: User demographics
        self.create_heart_rate()            # Step 8: Validated heart rate
        self.create_workouts()              # Step 9: Workout events
        self.create_completed_workouts()    # Step 10: Paired workout sessions
        self.create_workout_bpm()           # Step 11: Heart rate during workouts
        self.create_user_bins()             # Step 12: User demographic bins
        self.create_date_lookup()           # Step 13: Date dimension table
        # Gold layer tables/views
        self.create_workout_bpm_summary()   # Step 14: BPM summary aggregations
        self.create_gym_summary()           # Step 15: Gym usage summary view
        print(f"Setup completed in {int(time.time()) - start} seconds")
    
    # Helper method to assert a single table exists in the database
    # Queries SHOW TABLES and filters for the expected table name
    # Raises AssertionError if the table is missing
    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
    
    # Validates the entire setup by checking:
    #   1. The database exists in the catalog
    #   2. All 14 tables/views exist in the database
    # Prints success/failure for each check and total elapsed time
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        # First, verify the database itself exists
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        # Then verify each table/view exists
        self.assert_table("registered_users_bz")    # Bronze: user registrations
        self.assert_table("gym_logins_bz")           # Bronze: gym logins
        self.assert_table("kafka_multiplex_bz")      # Bronze: Kafka messages
        self.assert_table("users")                   # Silver: cleansed users
        self.assert_table("gym_logs")                # Silver: cleansed gym logs
        self.assert_table("user_profile")            # Silver: user demographics
        self.assert_table("heart_rate")              # Silver: heart rate readings
        self.assert_table("workouts")                # Silver: workout events
        self.assert_table("completed_workouts")      # Silver: completed sessions
        self.assert_table("workout_bpm")             # Silver: workout heart rate
        self.assert_table("user_bins")               # Silver: user bins
        self.assert_table("date_lookup")             # Silver: date dimension
        self.assert_table("workout_bpm_summary")     # Gold: BPM summary
        self.assert_table("gym_summary")             # Gold: gym usage view
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
    
    # Cleans up all resources created by setup:
    #   1. Drops the entire database with CASCADE (removes all tables/views)
    #   2. Deletes the raw data landing zone directory
    #   3. Deletes the streaming checkpoint directory
    # Safe to call even if some resources don't exist
    def cleanup(self): 
        # Only drop database if it exists (avoids errors on re-runs)
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")  # CASCADE drops all objects inside
            print("Done")
        # Remove the raw data landing zone (recursive delete)
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)  # True = recursive delete
        print("Done")
        # Remove streaming checkpoint files (recursive delete)
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)  # True = recursive delete
        print("Done")