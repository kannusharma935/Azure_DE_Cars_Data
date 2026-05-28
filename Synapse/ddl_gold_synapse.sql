-- Fact table
CREATE EXTERNAL TABLE gold.factsales(    
    Revenue INT,
    Units_Sold INT,
    RevPerUnit FLOAT,
    dim_branch_key INT,
    dim_model_key INT,
    dim_date_key INT,
    dim_dealer_key Int
)
WITH(
    LOCATION= 'factsales',
    DATA_SOURCE= gold_source,
    FILE_FORMAT= delta
)

SELECT * FROM gold.factsales;

--dim branch
CREATE EXTERNAL TABLE gold.dim_branch(    
    BranchName VARCHAR(100),
    Branch_ID VARCHAR(100),
    dim_branch_key INT
)
WITH(
    LOCATION= 'dim_branch',
    DATA_SOURCE= gold_source,
    FILE_FORMAT= delta
)

SELECT * FROM gold.dim_branch;

--dim date
CREATE EXTERNAL TABLE gold.dim_date(    
    Date_ID VARCHAR(100),
    dim_date_key INT
)
WITH(
    LOCATION= 'dim_date',
    DATA_SOURCE= gold_source,
    FILE_FORMAT= delta
)

SELECT * FROM gold.dim_date;


--dim dealer
CREATE EXTERNAL TABLE gold.dim_dealer(    
    DealerName VARCHAR(100),
    Dealer_ID VARCHAR(100),
    dim_dealer_key INT
)
WITH(
    LOCATION= 'dim_dealer',
    DATA_SOURCE= gold_source,
    FILE_FORMAT= delta
)

SELECT * FROM gold.dim_dealer;


--dim model
CREATE EXTERNAL TABLE gold.dim_model(    
    Model_ID VARCHAR(100),
    model_category VARCHAR(100),
    dim_model_key INT
)
WITH(
    LOCATION= 'dim_model',
    DATA_SOURCE= gold_source,
    FILE_FORMAT= delta
)

SELECT * FROM gold.dim_model;