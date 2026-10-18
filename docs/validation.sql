-- Run in Snowflake after the Databricks ETL job completes.
-- Update the table name if your target schema differs.

SELECT COUNT(*) AS row_count
FROM PROJECT_DB.SALES.K_SALES_RESULT;

SELECT
    SUM(TOTAL_AMOUNT) AS total_amount,
    SUM(TOTAL_DISCOUNT) AS total_discount,
    SUM(TOTAL_NET_AMOUNT) AS total_net_amount
FROM PROJECT_DB.SALES.K_SALES_RESULT;

-- Optional: inspect sample records
SELECT *
FROM PROJECT_DB.SALES.K_SALES_RESULT
LIMIT 20;
