-- Purpose: stg_customers cleans and standardises raw customer data for downstream use
-- Solves: Inconsistent formatting and data types from manual CSV uploads
-- Source: customers (raw table loaded by Airbyte from S3)
-- Grain: One row per customer
-- Excluded: Airbyte metadata columns (_airbyte_raw_id, _airbyte_extracted_at etc)
--           as they have no business value

SELECT 
    customer_id,
    TRIM("name") as customer_name,
    LOWER(trim(email)) as email,
    LOWER(TRIM(state)) as state,
    LOWER(TRIM(gender)) as gender,
    TRIM(address) as address,
    CAST(date_of_birth as DATE) as date_of_birth
from customers
where customer_id is not NULL

