
            -- Create table for NYC
            CREATE OR REPLACE TABLE Table_NYC (
                Unique_ID INT PRIMARY KEY,
                Patient_Name VARCHAR(255),
                Vaccine_Type CHAR(5),
                Date_of_Birth DATE,
                Date_of_Vaccination DATE,
                Age INT,
                Days_Since_Last_Consulted INT
            );

            -- Insert filtered data into the table
            INSERT INTO Table_NYC (
                Unique_ID, Patient_Name, Vaccine_Type, Date_of_Birth, Date_of_Vaccination, Age, Days_Since_Last_Consulted
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY Customer_Id) AS Unique_ID,
                Customer_Name AS Patient_Name,
                Vaccination_Id AS Vaccine_Type,
                DOB AS Date_of_Birth,
                Last_Consulted_Date AS Date_of_Vaccination,
                EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(DOB, 'DDMMYYYY')) AS Age,
                DATEDIFF(DAY, TO_DATE(Last_Consulted_Date, 'YYYYMMDD'), CURRENT_DATE) AS Days_Since_Last_Consulted
            FROM 
                staging_table
            WHERE 
                Country = 'NYC'
                AND DATEDIFF(DAY, TO_DATE(Last_Consulted_Date, 'YYYYMMDD'), CURRENT_DATE) > 30;
            