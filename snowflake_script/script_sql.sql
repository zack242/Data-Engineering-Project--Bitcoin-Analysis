-- Use the super admin role
USE ROLE accountadmin;

-- Create the warehouse
CREATE WAREHOUSE IF NOT EXISTS blockchain_wh 
    WITH WAREHOUSE_SIZE = 'Medium';

-- Create the database
CREATE DATABASE IF NOT EXISTS bitcoin_db;

-- Create the role and assign permissions
CREATE ROLE IF NOT EXISTS user_role;
GRANT USAGE ON WAREHOUSE blockchain_wh TO ROLE user_role;
GRANT ALL ON DATABASE bitcoin_db TO ROLE user_role;
GRANT ROLE user_role TO USER zack242; -- Replace with your username

use role user_role;

CREATE SCHEMA IF NOT EXISTS bitcoin_db.bitcoin_sch;
USE SCHEMA bitcoin_db.bitcoin_sch;

-- Assign the role to the user


-- Create the tables
CREATE TABLE IF NOT EXISTS bitcoin_db.bitcoin_sch.blocks (
    date VARCHAR(16777216),
    hash VARCHAR(16777216),
    size NUMBER(38,0),
    stripped_size NUMBER(38,0),
    weight NUMBER(38,0),
    number NUMBER(38,0),
    version NUMBER(38,0),
    merkle_root VARCHAR(16777216),
    timestamp VARCHAR(16777216),
    nonce NUMBER(38,0),
    bits VARCHAR(16777216),
    coinbase_param VARCHAR(16777216),
    transaction_count NUMBER(38,0),
    mediantime VARCHAR(16777216),
    difficulty FLOAT,
    chainwork VARCHAR(16777216),
    previousblockhash VARCHAR(16777216),
    ingestion_timestamp TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS bitcoin_db.bitcoin_sch.transactions (
    date VARCHAR(16777216),
    hash VARCHAR(16777216),
    size NUMBER(38,0),
    virtual_size NUMBER(38,0),
    version NUMBER(38,0),
    lock_time NUMBER(38,0),
    block_hash VARCHAR(16777216),
    block_number VARCHAR(16777216),
    block_timestamp VARCHAR(16777216),
    index NUMBER(38,0),
    input_count NUMBER(38,0),
    output_count NUMBER(38,0),
    input_value FLOAT,
    output_value FLOAT,
    is_coinbase BOOLEAN,
    fee FLOAT,
    inputs VARIANT,
    outputs VARIANT,
    ingestion_timestamp TIMESTAMP_LTZ
);

