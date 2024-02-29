-- Query details
-- This query returns details of people who submitted their application through the web-flow and stopped at a certain status for more than 1day.

-- Query Attributes
-- first_name = First name of the applicant 
-- last_name = Last name of the applicant 
-- phone_number = Phone number of the applicant
-- email = Email address of the applicant
-- offer_code = Offer code/Lead code of applicant
-- loan_id = Loan ID
-- unified_id = Unified ID
-- service_entity_name = Name of service entity
-- app_created_at_ct = Application created timestamp in Central Time
-- app_status = Status of app where it is stuck for a day
-- app_updated_at_ct = Timestamp when application reached this stage in Central Time
-- cadence_type = What is the cadence type i.e. Day 1,3,5
-- timestamp_ct = Timestamp when query was run in Central time

-- Table/View Name
-- business_analytics.communications.retargeting_email_data_dropoff_app_stage

-- Information regarding the borrower
WITH borrower_info AS (
    SELECT 
        b.id,
        bai.borrower_id,
        bai.loan_id,
        b.first_name,
        b.last_name,
        bai.phone_number,
        b.email
    FROM 
        above_dw_prod.above_public.borrowers b
    LEFT JOIN 
        above_dw_prod.above_public.borrower_aditional_info bai ON b.id = bai.borrower_id
    WHERE 
        NOT b._fivetran_deleted
        AND NOT bai._fivetran_deleted
) --SELECT * FROM borrower_info ;

-- Information regarding applications that drop of at particular stages of the web flow for the most recent application
, drop_offs AS (
    SELECT 
        l.id AS loan_id,
        l.unified_id,
        l.program_id,
        UPPER(l.code) AS offer_code,
        l.created_at AS app_created_at_utc,
        CONVERT_TIMEZONE('America/Chicago', l.created_at) AS app_created_at_ct,
        l.updated_at AS app_updated_at_utc,
        CONVERT_TIMEZONE('America/Chicago', l.updated_at) AS app_updated_at_ct,
        l.source_type,
        l.product_type,
        las.name AS app_status,
        bi.first_name,
        bi.last_name,
        bi.phone_number,
        bi.email
    FROM 
        above_dw_prod.above_public.loans l
    LEFT JOIN 
        above_dw_prod.above_public.loan_app_statuses las ON l.loan_app_status_id = las.id
    LEFT JOIN 
        borrower_info bi ON l.id = bi.loan_id
    WHERE 
        l.product_type = 'IPL'
        AND l.source_type = 'WEB'
        AND las.name IN ('BASIC_INFO_COMPLETE', 'OFFERED', 'OFFERED_SELECTED', 'ADD_INFO_COMPLETE') 
        AND l.created_at::DATE >= '2024-01-01'
        AND NOT l._fivetran_deleted
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY l.program_id ORDER BY l.created_at DESC) = 1
) --SELECT * FROM drop_offs;

-- Check to make sure the applications were still eligible when they applied
, drop_offs_w_eligibility AS (
    SELECT 
        loan_id,
        unified_id,
        offer_code,
        d.program_id,
        e.service_entity_name,
        app_created_at_utc,
        app_created_at_ct,
        app_updated_at_utc,
        app_updated_at_ct,
        source_type,
        product_type,
        app_status,
        first_name,
        last_name,
        phone_number,
        email
    FROM 
        drop_offs d
    LEFT JOIN 
        curated_prod.reporting.combined_eligibility_files e ON d.program_id = e.program_name
        AND d.app_created_at_ct::DATE >= e.calendar_date
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY d.loan_id ORDER BY e.calendar_date DESC) = 1
) --SELECT * FROM drop_offs_w_eligibility;

-- Add necessary fields with Cadence type and eliminate is cadence type is null
, drop_off_final_table AS (
    SELECT 
        first_name,
        last_name,
        phone_number,
        email,
        offer_code,
        loan_id,
        unified_id,
        service_entity_name,
        app_created_at_ct,
        app_status,
        app_updated_at_ct,
        CASE 
            WHEN CURRENT_DATE - app_updated_at_ct::DATE = 1 THEN 'Day 1'
            WHEN CURRENT_DATE - app_updated_at_ct::DATE = 3 THEN 'Day 3'
            WHEN CURRENT_DATE - app_updated_at_ct::DATE = 5 THEN 'Day 5'
        END AS cadence_type,
        CONVERT_TIMEZONE('America/Chicago',CURRENT_TIMESTAMP)::TIMESTAMP_NTZ AS Timestamp_ct
    FROM 
        drop_offs_w_eligibility
    WHERE 
        convert_timezone('America/Chicago',current_timestamp)::DATE - app_updated_at_ct::DATE>=1
        AND cadence_type IS NOT NULL
    -- ASK TAYLOR ENTITY NAME FILTER
    -- AND service_entity_name = 'Beyond Finance'
    ORDER BY 
        cadence_type, app_status, app_created_at_ct
)

Select *
from drop_off_final_table
;


Select *
from business_analytics.communications.retargeting_email_data_dropoff_app_stage