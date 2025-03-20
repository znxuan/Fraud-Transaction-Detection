-- Step1: Encoding Categorical Features
-- the Fraud Source Feature

CREATE TABLE `techteam-448216.fraud_detection.fraud_source_encoded` AS
SELECT
  source,
  CASE
    WHEN source = 'INTERNET' THEN 0
    WHEN source = 'TELEAPP' THEN 1
    ELSE NULL
  END AS source_encoded,
  * EXCEPT (source)
FROM `techteam-448216.fraud_detection.Fraud Detection`;


-- Encoding the DeviceOS feature


CREATE TABLE `techteam-448216.fraud_detection.fraud_deviceos_encoded` AS
WITH target_encoding AS (
  SELECT
    device_os,
    AVG(fraud_bool) AS device_os_encoded
  FROM
    `techteam-448216.fraud_detection.fraud_source_encoded`
  GROUP BY
    device_os
)

SELECT
  t.device_os_encoded,
  original.*
FROM
  `techteam-448216.fraud_detection.fraud_source_encoded` original
JOIN
  target_encoding t
ON
  original.device_os = t.device_os;



--Encoding the housing status feature



CREATE TABLE `techteam-448216.fraud_detection.fraud_housing_status_encoded` AS
SELECT
    *,
    IF(housing_status = 'BA', 1, 0) AS housing_status_A,
    IF(housing_status = 'BB', 1, 0) AS housing_status_B,
    IF(housing_status = 'BC', 1, 0) AS housing_status_C,
    IF(housing_status = 'BD', 1, 0) AS housing_status_D,
    IF(housing_status = 'BE', 1, 0) AS housing_status_E,
    IF(housing_status = 'BF', 1, 0) AS housing_status_F,
    IF(housing_status = 'BG', 1, 0) AS housing_status_G
FROM `techteam-448216.fraud_detection.fraud_deviceos_encoded`;



--Encoding the employment status feature


CREATE TABLE `techteam-448216.fraud_detection.fraud_employment_status_encoded` AS
SELECT
    *,
    IF(employment_status = 'CD', 1, 0) AS employment_status_1,
    IF(employment_status = 'CB', 1, 0) AS employment_status_2,
    IF(employment_status = 'CA', 1, 0) AS employment_status_3,
    IF(employment_status = 'CF', 1, 0) AS employment_status_4,
    IF(employment_status = 'CE', 1, 0) AS employment_status_5,
    IF(employment_status = 'CC', 1, 0) AS employment_status_6,
    IF(employment_status = 'CG', 1, 0) AS employment_status_7
FROM `techteam-448216.fraud_detection.fraud_housing_status_encoded`;



-- Encoding the Payment Type Feature



CREATE TABLE `techteam-448216.fraud_detection.fraud_payment_type_encoded` AS
SELECT
    *,
    IF(payment_type = 'AA', 1, 0) AS payment_type_A,
    IF(payment_type = 'AB', 1, 0) AS payment_type_B,
    IF(payment_type = 'AC', 1, 0) AS payment_type_C,
    IF(payment_type = 'AD', 1, 0) AS payment_type_D,
    IF(payment_type = 'AE', 1, 0) AS payment_type_E
FROM `techteam-448216.fraud_detection.fraud_employment_status_encoded`;



-- Normalizing all the numerical features


CREATE  TABLE `techteam-448216.fraud_detection.normalized_numerics` AS
SELECT 
  -- Z-score Normalization
  (session_length_in_minutes - AVG(session_length_in_minutes) OVER()) / STDDEV(session_length_in_minutes) OVER() AS session_length_in_minutes,
  (credit_risk_score - AVG(credit_risk_score) OVER()) / STDDEV(credit_risk_score) OVER() AS credit_risk_score,
  (intended_balcon_amount - AVG(intended_balcon_amount) OVER()) / STDDEV(intended_balcon_amount) OVER() AS intended_balcon_amount,
  (zip_count_4w - AVG(zip_count_4w) OVER()) / STDDEV(zip_count_4w) OVER() AS zip_count_4w,
  (velocity_6h - AVG(velocity_6h) OVER()) / STDDEV(velocity_6h) OVER() AS velocity_6h,
  (velocity_24h - AVG(velocity_24h) OVER()) / STDDEV(velocity_24h) OVER() AS velocity_24h,
  (velocity_4w - AVG(velocity_4w) OVER()) / STDDEV(velocity_4w) OVER() AS velocity_4w,
  (bank_branch_count_8w - AVG(bank_branch_count_8w) OVER()) / STDDEV(bank_branch_count_8w) OVER() AS bank_branch_count_8w,

  -- Min-Max Normalization
  (proposed_credit_limit - MIN(proposed_credit_limit) OVER()) / (MAX(proposed_credit_limit) OVER() - MIN(proposed_credit_limit) OVER()) AS proposed_credit_limit,
  (date_of_birth_distinct_emails_4w - MIN(date_of_birth_distinct_emails_4w) OVER()) / (MAX(date_of_birth_distinct_emails_4w) OVER() - MIN(date_of_birth_distinct_emails_4w) OVER()) AS date_of_birth_distinct_emails_4w,
  (customer_age - MIN(customer_age) OVER()) / (MAX(customer_age) OVER() - MIN(customer_age) OVER()) AS customer_age,
  (prev_address_months_count - MIN(prev_address_months_count) OVER()) / (MAX(prev_address_months_count) OVER() - MIN(prev_address_months_count) OVER()) AS prev_address_months_count,
  (current_address_months_count - MIN(current_address_months_count) OVER()) / (MAX(current_address_months_count) OVER() - MIN(current_address_months_count) OVER()) AS current_address_months_count,
  (bank_months_count - MIN(bank_months_count) OVER()) / (MAX(bank_months_count) OVER() - MIN(bank_months_count) OVER()) AS bank_months_count,
  (month - MIN(month) OVER()) / (MAX(month) OVER() - MIN(month) OVER()) AS month,

  -- Keep non-normalized columns as they are
  income,
  name_email_similarity,
  days_since_request,
  device_fraud_count,
  device_distinct_emails_8w,
  device_os_encoded,
  source,
  source_encoded,
  fraud_bool,
  payment_type,
  employment_status,
  email_is_free,
  housing_status,
  phone_home_valid,
  phone_mobile_valid,
  has_other_cards,
  foreign_request,
  device_os,
  keep_alive_session,
  housing_status_A,
  housing_status_B,
  housing_status_C,
  housing_status_D,
  housing_status_E,
  housing_status_F,
  housing_status_G,
  employment_status_1,
  employment_status_2,
  employment_status_3,
  employment_status_4,
  employment_status_5,
  employment_status_6,
  employment_status_7,
  payment_type_A,
  payment_type_B,
  payment_type_C,
  payment_type_D,
  payment_type_E
FROM `techteam-448216.fraud_detection.fraud_payment_type_encoded`;

-- Step 2:Create Training, Validation, and Testing Tables

-- Create Training Table (70%)
CREATE TABLE `techteam-448216.fraud_detection.fraud_train` AS
WITH stratified_split AS (
    SELECT *, RAND() AS random_value
    FROM `techteam-448216.fraud_detection.normalized_numerics`
)
SELECT * FROM stratified_split WHERE random_value < 0.7 AND fraud_bool = 0
UNION ALL
SELECT * FROM stratified_split WHERE random_value < 0.7 AND fraud_bool = 1;

-- Create Validation Table (10%)
CREATE TABLE `techteam-448216.fraud_detection.fraud_valid` AS
WITH stratified_split AS (
    SELECT *, RAND() AS random_value
    FROM `techteam-448216.fraud_detection.normalized_numerics`
)
SELECT * FROM stratified_split WHERE random_value >= 0.7 AND random_value < 0.9 AND fraud_bool = 0
UNION ALL
SELECT * FROM stratified_split WHERE random_value >= 0.7 AND random_value < 0.9 AND fraud_bool = 1;

-- Create Testing Table (10%)
CREATE TABLE `techteam-448216.fraud_detection.fraud_test` AS
WITH stratified_split AS (
    SELECT *, RAND() AS random_value
    FROM `techteam-448216.fraud_detection.normalized_numerics`
)
SELECT * FROM stratified_split WHERE random_value >= 0.9 AND fraud_bool = 0
UNION ALL
SELECT * FROM stratified_split WHERE random_value >= 0.9 AND fraud_bool = 1;

-- Step 3:Feature Engineering (Repeat this for Train/Valid/Test)

CREATE TABLE `techteam-448216.fraud_detection.train_resampled_fengineered` AS
SELECT 
    *,
    -- New Feature: Income per Age
    income / CASE 
               WHEN customer_age = 0 THEN 1e-3 
               ELSE customer_age 
             END AS income_per_age,

    -- New Feature: Session Velocity Ratio
    session_length_in_minutes / velocity_24h AS session_velocity_ratio,

    -- New Feature: Address Permanency
    current_address_months_count / CASE 
                                      WHEN prev_address_months_count = 0 THEN 1e-5 
                                      ELSE prev_address_months_count 
                                   END AS address_permanency,

    -- New Feature: Email & Phone Validity Interaction
    email_is_free * phone_home_valid AS email_phone_valid,

    -- New Feature: Credit Risk Score per Bank Months Count
    credit_risk_score / CASE 
                           WHEN bank_months_count = 0 THEN 1e-5 
                           ELSE bank_months_count 
                        END AS credit_bank_months_ratio

FROM `techteam-448216.fraud_detection.train_resampled`;

