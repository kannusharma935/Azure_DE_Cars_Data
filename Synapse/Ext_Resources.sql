--  external data source,  system managed identity, database scoped cred  

-- CREATE MASTER KEY ENCRYPTION BY PASSWORD=''

CREATE DATABASE SCOPED CREDENTIAL aka_creds
WITH IDENTITY='Managed Identity'



--EXternal File Format
CREATE EXTERNAL FILE FORMAT parquet
WITH(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
)



CREATE EXTERNAL FILE FORMAT delta
WITH(
    FORMAT_TYPE=DELTA
)

CREATE EXTERNAL DATA SOURCE gold_source
WITH(
    LOCATION= '',
    CREDENTIAL= aka_creds
)





