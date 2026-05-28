CREATE TABLE cars_data(
    Branch_ID VARCHAR(100),
    Dealer_ID VARCHAR(100),
    Model_ID VARCHAR(200),
    Revenue BIGINT,
    Units_Sold BIGINT,
    Date_ID VARCHAR(200),
    Day INT,
    Month INT,
    Year INT,
    BranchName VARCHAR(2000),
    DealerName VARCHAR(2000)
)

SELECT * FROM cars_data;

-- CREATE TABLE Water_mark_table(
--     last_load VARCHAR(2000)
--     )

--   creating stored procedure
CREATE PROCEDURE UpdateWatermarkTable
    @lastload VARCHAR(200)
AS
BEGIN
    --Start Transaction
    BEGIN TRANSACTION;

    --Update the incremental column in the table
    UPDATE Water_mark_table
    SET last_load=@lastload
    COMMIT TRANSACTION;
    END;    
