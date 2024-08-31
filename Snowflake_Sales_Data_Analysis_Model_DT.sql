--Sales Data Analysis Model
--1) The provided Snowflake script sets up a comprehensive sales data model which includes tables for 
--customers, buyers, clients, and opportunities, along with sample data and various tagging and role-based access controls (RBAC). 

--2) With this model, several types of analyses can be performed. 
--For instance, we can analyze customer behavior and segmentation by evaluating closed opportunities, 
--categorizing customers based on their total value, and tracking sales stages to identify bottlenecks in the sales pipeline. 

--3)Additionally, the tagging system for Personally Identifiable Information (PII) and lead sources enhances data governance 
--and compliance. By leveraging stored procedures and views, analysts can easily access high-value customer insights 
--and opportunities likely to close soon, facilitating more informed decision-making and targeted marketing strategies. 

--4)This model also supports efficient role-based access control, ensuring that different user roles have 
--appropriate access to the data. The model's structure, sample data, and defined procedures facilitate 
--a hands-on understanding of Snowflake's capabilities for data management, analysis, and security.
/*
------------------------------------------------------------------------------
-- Snowflake Demo Script: Sales Data Model and Universal Search Exploration
-- 
-- Description: 
-- This script sets up a sales data model in Snowflakes. It includes the creation of tables for customers, buyers,
-- clients, and opportunities, along with sample data insertion and tagging of 
-- columns for PII, lead source, and sales stage. Additionally, it defines RBAC 
-- privileges, functions, stored procedures, and views for analysis purposes.
--
-- Author: Fru N.
-- Website: DemoHub.dev
--
-- Date: May 15, 2024
--
-- Copyright: (c) 2024 DemoHub.dev. All rights reserved.
--
-- Disclaimer:  
-- This script is for educational and demonstration purposes only. It is not
-- affiliated with or endorsed by Snowflake Computing. Use this code at your 
-- own risk.
------------------------------------------------------------------------------
*/
-- +----------------------------------------------------+
-- |             1. DATABASE AND SCHEMA SETUP           |
-- +----------------------------------------------------+

-- Creates a new database named "SalesDB".
CREATE OR REPLACE DATABASE SalesDB;

-- Use the newly created database.
USE SalesDB;

-- Create a schema within the database.
CREATE OR REPLACE SCHEMA Custs;

-- +----------------------------------------------------+
-- |             2. CREATE TABLE OBJECTS                |
-- +----------------------------------------------------+

-- Create tables for storing customer, buyer, client, and opportunity data.

-- Customer Table
CREATE OR REPLACE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    VarNumber VARCHAR(20),
    Email VARCHAR(100),
    HomeLocation VARCHAR(200),
    ZipCode VARCHAR(10),
    LoadDate TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()  
);

-- Buyer Table
CREATE OR REPLACE TABLE Buyer (
    BuyerID INT PRIMARY KEY,
    CustomerID INT REFERENCES Customer(CustomerID),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Address VARCHAR(200),
    PostalCode VARCHAR(10),
    LoadDate TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()  
);

-- Client Table
CREATE OR REPLACE TABLE Client (
    ClientID INT PRIMARY KEY,
    BuyerID INT REFERENCES Buyer(BuyerID),
    ContractStartDate DATE,
    ContractValue DECIMAL(10, 2),
    LoadDate TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()  
);

-- Opportunities Table
CREATE OR REPLACE TABLE Opportunities (
    OpportunityID INT PRIMARY KEY,
    CustomerID INT REFERENCES Customer(CustomerID),
    BuyerID INT REFERENCES Buyer(BuyerID),
    LeadSource VARCHAR(50),
    SalesStage VARCHAR(20),
    ExpectedCloseDate DATE,
    Amount DECIMAL(10, 2),
    LoadDate TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()  
);

-- +----------------------------------------------------+
-- |             3. INSERT SAMPLE DATA                  |
-- +----------------------------------------------------+

-- Insert sample data into the created tables.

-- Customer Data Insertion
INSERT INTO Customer (CustomerID, FirstName, LastName, Email, HomeLocation, ZipCode, VarNumber, LoadDate)
VALUES
    (1, 'Alice', 'Johnson', 'alice.johnson@example.com', '123 Oak St', '94105', 'LTY-12345', '2024-04-20 10:30:00'),  -- Valid
    (2, 'Bob', 'Smith', 'bob.smith@example.com', '456 Elm St', '10001', 'LTY-23456', '2024-03-15 11:15:00'),        -- Valid
    (3, 'Eva', 'Davies', 'eva.davis@example.com', '789 Maple Ave', '20001', 'LTY-34567', '2024-02-10 14:45:00'),   -- Valid
    (4, 'Dave', 'Brown', 'dave.brown@example.com', '123 Main St', '54321', 'LTY-45678', '2023-12-30 09:20:00'),   -- Stale Data
    (5, 'Emily', 'White', 'invalid_email', '456 Park Ave', '67890', 'LTY-56789', '2024-05-15 16:55:00'),        -- Invalid Email
    (6, NULL, 'Wilson', 'charlie.wilson@example.com', '789 Broadway', '87654', 'LTY-67890', '2024-05-18 12:00:00'), -- Missing First Name
    (7, 'Grace', 'Lee', 'grace.lee@example.com', NULL, '34567', 'LTY-78901', '2024-05-10 08:50:00'),         -- Missing HomeLocation
    (8, 'Henry', 'Miller', 'henry.miller@example.com', '1011 Market St', '', 'LTY-89012', '2024-05-05 15:35:00'),  -- Missing ZipCode
    (9, 'Ivy', 'Tailor', 'alice.johnson@example.com', '5566 Sunset Blvd', '12345', 'LTY-90123', '2024-05-19 18:10:00'), -- Duplicate Email (same as Alice). Wrong spelling of tailor
    (10, 'Eva', 'Davis', 'eva.davis@anderson.com', '2233 River Rd', '98765', 'LTY-01234', '2024-05-01 13:25:00');   -- Valid


-- Buyer Data Insertion
INSERT INTO Buyer (BuyerID, CustomerID, FirstName, LastName, Email, Address, PostalCode, LoadDate)
VALUES
    (101, 1, 'Alice', 'Johnson', 'alice.johnson@example.com', '123 Oak St', '94105', '2024-04-25 12:30:00'),   -- Valid
    (102, 2, 'Bob', 'Smith', 'bob.smith@example.com', '456 Elm St', '10001', '2024-03-20 15:45:00'),      -- Valid
    (103, NULL, 'David', 'Lee', 'david.lee@example.com', '987 Pine St', '33101', '2024-02-15 11:20:00'),   -- Valid, No CustomerID
    (104, 4, 'Dave', 'Brown', 'dave.brown@example.com', '123 Main St', '54321', '2023-12-31 14:30:00'),   -- Stale Data
    (105, 5, 'Emily', 'White', 'invalid_email2', '456 Park Ave', '67890', '2024-05-16 10:15:00'),        -- Invalid Email
    (106, 6, NULL, 'Wilson', 'charlie.wilson@example.com', '789 Broadway', '87654', '2024-05-19 09:45:00'),-- Missing First Name
    (107, 7, 'Grace', 'Li', 'grace.lee@example.com', NULL, '34567', '2024-05-11 16:25:00'),            -- Missing Address
    (108, 8, 'Henry', 'Mila', 'henry.miller@example.com', '1011 Market St', '', '2024-05-06 13:00:00'),  -- Missing PostalCode. Wrong spelling of mller
    (109, 9, 'Ivy', 'Taylor', 'ivy.taylor@example.com', '5566 Sunset Blvd', '12345', '2024-05-20 17:50:00'), -- Duplicate Email (same as Alice)
    (110, 10, 'Jack', 'Anderson', 'jack@anderson.com', '2233 River Rd', '98765', '2024-05-02 18:05:00');    -- Valid



-- Client Data Insertion
INSERT INTO Client (ClientID, BuyerID, ContractStartDate, ContractValue, LoadDate)
VALUES 
    (201, 101, '2024-01-15', 50000, '2024-01-15 10:30:00'),
    (202, 102, '2023-12-01', 85000, '2023-12-01 11:15:00'),
    (203, 103, '2024-03-10', 60000, '2024-03-10 14:45:00'),
    (204, 104, '2023-11-20', 75000, '2023-11-20 09:20:00'),   -- Stale data
    (205, 105, '2024-04-30', NULL, '2024-04-30 16:55:00'),    -- Missing ContractValue
    (206, NULL, '2024-02-28', 90000, '2024-02-28 12:00:00'), -- Missing BuyerID
    (207, 107, '2024-05-05', -5000, '2024-05-05 08:50:00'),    -- Negative ContractValue
    (208, 108, '2024-01-01', 120000, '2024-01-01 15:35:00'),
    (209, 109, '2023-12-15', 100000, '2023-12-15 18:10:00'),
    (210, 110, '2024-04-10', 45000, '2024-04-10 13:25:00');



-- Opportunities Data Insertion
INSERT INTO Opportunities (OpportunityID, CustomerID, BuyerID, LeadSource, SalesStage, ExpectedCloseDate, Amount, LoadDate)
VALUES 
    (1003, 3, NULL, 'Outbound Call', 'Qualification', '2024-05-20', 75000, '2024-05-12 18:15:00'),     -- Valid
    (1004, NULL, 103, 'Email Campaign', 'Proposal', '2024-06-10', 90000, '2024-04-28 14:20:00'),      -- Valid, No CustomerID
    (1005, 4, NULL, 'Web Form', 'Negotiation', '2024-07-15', 150000, '2024-05-19 11:35:00'),      -- Valid
    (1006, 5, NULL, 'Partner Referral', 'InvalidStage', '2024-08-20', 180000, '2024-05-22 16:40:00'),  -- Invalid SalesStage
    (1007, 6, NULL, 'Email Campaign', 'Qualification', NULL, 65000, '2024-05-17 13:50:00'),         -- Missing ExpectedCloseDate
    (1008, 7, 107, 'Cold Call', 'Closed Won', '2024-05-10', 110000, '2023-12-28 08:30:00'),         -- Stale Data (Closed Won but old LoadDate)
    (1009, 8, 108, 'Webinar', 'Closed Lost', '2024-03-25', NULL, '2024-04-15 15:10:00'),          -- Missing Amount
    (1010, 9, NULL, 'Referral', 'Proposal', '2024-06-05', -80000, '2024-05-14 17:25:00'),         -- Negative Amount
    (1011, 10, 110, 'Social Media', 'Negotiation', '2024-07-02', 135000, '2024-05-08 12:45:00'),    -- Valid
    (1012, 2, 102, 'Email Campaign', 'Closed Won', '2023-12-01', 85000, '2024-05-21 10:00:00');       -- Duplicate Opportunity


-- +----------------------------------------------------+
-- |             4. CREATE AND INSERT COMMENTS          |
-- +----------------------------------------------------+

-- Add comments to tables and columns for better understanding.

-- Customer Table Comments
COMMENT ON TABLE Customer IS 'Stores information about our potential and existing customers, including contact details and location.';
COMMENT ON COLUMN Customer.HomeLocation IS 'The customer''s primary residence address.';

-- Buyer Table Comments
COMMENT ON TABLE Buyer IS 'Tracks information about customers who have made a purchase, including their new address and postal code.';
COMMENT ON COLUMN Buyer.Address IS 'The buyer''s preferred shipping or billing address.';
COMMENT ON COLUMN Buyer.PostalCode IS 'The buyer''s postal code, which may differ from their home location zip code.';

-- Client Table Comments
COMMENT ON TABLE Client IS 'Stores details about customers who have signed contracts, including contract start date and value.';
COMMENT ON COLUMN Client.ContractValue IS 'The total monetary value of the signed contract.';

-- Opportunities Table Comments
COMMENT ON TABLE Opportunities IS 'Tracks the progress of sales opportunities, including lead source, current sales stage, expected close date, and potential value.';
COMMENT ON COLUMN Opportunities.LeadSource IS 'How the lead was initially acquired (e.g., referral, web form, cold call).';
COMMENT ON COLUMN Opportunities.SalesStage IS 'The current stage of the opportunity in the sales pipeline.';

-- +----------------------------------------------------+
-- |     5. CREATE FUNCTIONS, STORED PROCS AND VIEWS    |
-- +----------------------------------------------------+

-- Define functions, stored procedures, and views for analysis purposes.

-- Function to calculate the total value of closed opportunities for a given customer
CREATE OR REPLACE FUNCTION customer_closed_won_value(customer_id INT) RETURNS NUMBER(19,4) LANGUAGE SQL
AS
$$
SELECT SUM(o.Amount)
FROM Opportunities o
JOIN Customer c ON o.CustomerID = c.CustomerID
WHERE o.CustomerID = customer_id
AND o.SalesStage = 'Closed Won'
$$;

-- Function to categorize customers based on their total value of closed opportunities
CREATE OR REPLACE FUNCTION categorize_customer (customer_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
SELECT CASE 
    WHEN customer_closed_won_value(customer_id) >= 100000 THEN 'High Value'
    WHEN customer_closed_won_value(customer_id) >= 50000 THEN 'Medium Value'
    ELSE 'Low Value'
END
$$;


-- Stored procedure to update the sales stage of an opportunity
CREATE OR REPLACE PROCEDURE update_opportunity_stage (opportunity_id INT, new_stage VARCHAR)
  RETURNS STRING
  LANGUAGE SQL
AS $$
BEGIN
    UPDATE SalesDB.customer.Opportunities
    SET SalesStage = new_stage
    WHERE OpportunityID = opportunity_id;
    RETURN 'Success'; -- or any message you want to return upon successful execution
END;
$$;


-- Stored procedure to assign a new buyer to a customer
CREATE OR REPLACE PROCEDURE assign_buyer_to_customer (customer_id INT, buyer_id INT)
  RETURNS STRING
  LANGUAGE SQL
AS $$
BEGIN
    UPDATE SalesDB.customer.Customer
    SET BuyerID = buyer_id
    WHERE CustomerID = customer_id;
END;
$$;

-- View to display high-value customers
CREATE OR REPLACE VIEW high_value_customers AS
SELECT c.*, customer_closed_won_value(c.CustomerID) AS TotalValue
FROM Customer c;
--WHERE categorize_customer(c.CustomerID) = 'High Value';

-- View to display opportunities likely to close in the next month
CREATE OR REPLACE VIEW opportunities_likely_to_close AS
SELECT *
FROM Opportunities
WHERE SalesStage IN ('Negotiation', 'Proposal')
AND ExpectedCloseDate BETWEEN CURRENT_DATE AND DATEADD(month, 1, CURRENT_DATE);


-- +----------------------------------------------------+
-- |             6. CREATE TAGS AND APPLY               |
-- +----------------------------------------------------+
-- -- Define tags for PII, lead source, and sales stage and apply them to relevant columns.

create or REPLACE tag cost_center
    allowed_values 'finance', 'engineering';

-- Create Tag for Personally Identifiable Information (PII)
CREATE or REPLACE TAG PII ALLOWED_VALUES 'Name', 'Email', 'Address' COMMENT = 'Indicates personally identifiable information';

-- Create Tag for Lead Source
CREATE or REPLACE TAG Lead_Source ALLOWED_VALUES 'Partner Referral', 'Web Form', 'Outbound Call', 'Trade Show' COMMENT = 'Indicates the source of the lead or opportunity';

-- Create Tag for Sales Stage
CREATE or REPLACE TAG Sales_Stage ALLOWED_VALUES 'Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost' COMMENT = 'Indicates the current stage of the sales opportunity';

-- Apply Tags to Tables and Columns

-- Customer Table
ALTER TABLE Customer MODIFY COLUMN FirstName SET TAG PII = 'Name';
ALTER TABLE Customer MODIFY COLUMN LastName SET TAG PII = 'Name';
ALTER TABLE Customer MODIFY COLUMN Email SET TAG PII = 'Email';


-- Buyer Table
ALTER TABLE Buyer MODIFY COLUMN FirstName SET TAG PII = 'Name';
ALTER TABLE Buyer MODIFY COLUMN LastName SET TAG PII = 'Name';
ALTER TABLE Buyer MODIFY COLUMN Email SET TAG PII = 'Email';
ALTER TABLE Buyer MODIFY COLUMN Address SET TAG PII = 'Address';


-- Client Table: Should FAIL. Missing Column
ALTER TABLE Client MODIFY COLUMN Address SET TAG PII = 'Address';

-- Opportunities Table: Should FAIL. Invalid Tag Values. 
ALTER TABLE Opportunities MODIFY COLUMN LeadSource SET TAG Lead_Source = 'LeadSource';
ALTER TABLE Opportunities MODIFY COLUMN SalesStage SET TAG Sales_Stage = 'SalesStage';


-- +----------------------------------------------------+
-- |     7. CREATE MASKING AND APPLY TO TAGS/COLUMNS    |
-- +----------------------------------------------------+
USE ROLE ACCOUNTADMIN;

-- Create a masking policy for PII data
CREATE OR REPLACE MASKING POLICY mask_pii AS (val string) RETURNS string ->
    CASE
        WHEN current_role() IN ('SalesManager') THEN val -- Full access for SalesManager
        ELSE '***MASKED***'   -- Mask for all other roles
    END;

-- Apply the masking policy to columns tagged with PII
ALTER TAG PII SET MASKING POLICY mask_pii;
-- ... (Apply to other PII columns in Buyer and Client tables)

-- +----------------------------------------------------+
-- |             8. RBAC PRIVILEGES SETUP               |
-- +----------------------------------------------------+

-- Define roles and grant privileges for role-based access control.

-- Use a role with sufficient privileges
USE ROLE ACCOUNTADMIN;

-- Create Roles
CREATE OR REPLACE ROLE SalesRep;
CREATE OR REPLACE ROLE SalesManager;

-- Grant Usage on Database to Roles
GRANT USAGE ON DATABASE SalesDB TO ROLE SalesRep;
GRANT USAGE ON DATABASE SalesDB TO ROLE SalesManager;

-- Grant Usage on Schema to Roles
GRANT USAGE ON SCHEMA SalesDB.custs TO ROLE SalesRep;
GRANT USAGE ON SCHEMA SalesDB.custs TO ROLE SalesManager;

-- Grant Usage on Warehouse to Roles
GRANT USAGE ON WAREHOUSE Demo_WH TO ROLE SalesRep;
GRANT USAGE ON WAREHOUSE Demo_WH TO ROLE SalesManager;

-- Grant Select on Tables to Roles
GRANT SELECT ON TABLE Customer TO ROLE SalesRep;
GRANT SELECT ON ALL TABLES IN SCHEMA custs TO ROLE SalesManager; -- More access

-- +----------------------------------------------------+
-- |       9. SWITCH TO THE ACCOUNTADMIN ROLE           |
-- +----------------------------------------------------+

-- When Demoing or testing this; switch to the user called DEMO. 
-- Make sure to switch the role from default to SALES REP. Perform Search with universal search
USE ROLE ACCOUNTADMIN;

-- Grant the SalesRep role to the Fru user (Admin)
GRANT ROLE SalesManager TO USER Fru;

-- Grant the SalesRep role to the Demo user
GRANT ROLE SalesRep TO USER Demo;


-- +----------------------------------------------------+
-- |             10. RESET DEMO ENVIRONMENT              |
-- +----------------------------------------------------+

--USE ROLE ACCOUNTADMIN;  -- Or a role with sufficient privileges

-- Drop the database
--DROP DATABASE IF EXISTS SalesDB CASCADE;

-- Revoke from user
--REVOKE ROLE SalesRep FROM USER Demo;

-- Drop the roles
--DROP ROLE IF EXISTS SalesRep;
--DROP ROLE IF EXISTS SalesManager;