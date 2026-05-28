--  external data source,  system managed identity, database scoped cred  

-- CREATE MASTER KEY ENCRYPTION BY PASSWORD='password@123456'

CREATE DATABASE SCOPED CREDENTIAL aka_creds
WITH IDENTITY='Managed Identity'


CREATE EXTERNAL DATA SOURCE silver_source
WITH(
    LOCATION= 'https://azuredwhengglake.dfs.core.windows.net/silver/',
    CREDENTIAL= aka_creds
)


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
    LOCATION= 'https://azuredwhengglake.dfs.core.windows.net/gold/',
    CREDENTIAL= aka_creds
)





