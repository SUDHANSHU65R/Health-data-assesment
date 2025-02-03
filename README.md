# Hospital Patient Data Processor

## Project Overview
The Hospital Patient Data Processor is a Python-based ETL (Extract, Transform, Load) tool designed to handle patient data for a multi-specialty hospital chain. This tool processes patient data files, extracts relevant information, transforms it according to specified rules, and loads it into country-specific output files. Additionally, it generates SQL table creation queries for each country-specific table.

## Features
- **Data Extraction:** Reads patient data from a specified file.
- **Data Transformation:** Processes data to include age and days since the last consultation, filters records, and handles missing values.
- **Data Loading:** Splits data into country-specific CSV files with required columns and monotonically increasing `Unique_ID`.
- **Validation:** Ensures data integrity by dropping records with null values in mandatory fields.
- **SQL Queries:** Generates SQL queries for creating tables for each country-specific dataset.

## Project Structure
Health-data-assesment/  
├── data/  
│   └── patient_data.txt  
├── output/  
│   └── [Generated CSV Files]  
│   └── /raw data/cleaned patient data.csv  
│   └── /sql_queries/[Table_{Country}_create_insert.sql]  
├── src/  
│   └── patient_processor.py  
│   └── main.py  
├── tests/  
│   └── test_patient_processor.py  
└── README.md  


## Usage
1. **Prepare the Data File:**
- Place your data file in the `data/` directory. Ensure it follows the specified format.

2. **Run the Processor:**  
```python src/patient_processor.py```


3. **Check the Output:**
- Processed data will be saved in the `output/` directory as country-specific CSV files.

## Data Format
### Input Data File
The input data file (`patient_data.txt`) should follow this format:  
| H  | Customer_Name | Customer_Id | Open_Date | Last_Consulted_Date | Vaccination_Id | Dr_Name | State | Country | DOB      | Is_Active |
|----|--------------|-------------|-----------|----------------------|----------------|---------|-------|---------|----------|-----------|
| D  | Alex        | 123457      | 20101012  | 20121013             | MVD            | Paul    | SA    | USA     | 06031987 | A         |
| D  | John        | 123458      | 20101012  | 20121013             | MVD            | Paul    | TN    | IND     | 06031987 | A         |
| D  | Mathew      | 123459      | 20101012  | 20121013             | MVD            | Paul    | WAS   | PHIL    | 06031987 | A         |
| D  | Matt        | 12345       | 20101012  | 20121013             | MVD            | Paul    | BOS   | NYC     | 06031987 | A         |
| D  | Jacob       | 1256        | 20101012  | 20121013             | MVD            | Paul    | VIC   | AU      | 06031987 | A         |


### Output Data Files
Each country-specific output file (`Table_{country}.csv`) will contain the following columns:
- **Unique_ID**: Monotonically increasing ID
- **Patient Name**: Customer's name
- **Vaccine Type**: Type of vaccination
- **Date of Birth**: Customer's date of birth
- **Date of Vaccination**: Date of the last consultation
-**Age**: Age of the customer
-**Days_Since_Last_Consulted**: No of days since last consulattion

## Process Flow
1. **Reading Data:**
   - The data is read from the input file in chunks to handle large datasets efficiently.

2. **Data Transformation:**
   - Records are filtered and transformed to calculate age and days since the last consultation.
   - Records with null values in mandatory fields are dropped.
   - After processing raw data is stored in `output/raw data` directory for staging purposes.

3. **Data Filtering:**
   - Records where `Days_Since_Last_Consulted` > 30 are kept.

4. **Splitting Data:**
   - Data is split into country-specific groups.
   - Each group is saved as a CSV file with monotonically increasing `Unique_ID`.

5. **Country wise data and query:**
  - Country wise data has been saved in `output` directory after applying filters for `Last_Consulted_Date > 30`
  - Queries for creating tables and inserting data from staging table has been generated dynamically and stored in `output/sql_query` directory.
  - **if we are considering snowflake to store data then we dont need countery wise csv data since queries will create tables and fetch data from snowflake staged table.**
  - **Country wise csv data has been provided only for demonstration purpose.**

## Test Cases
Pytest test cases are provided to validate the functionality of the data processor. Run the tests using:

```pytest -v tests/test_patient_processor.py```

## Consideration
We can consider pyspark for distributed computation in cloud environment like EMR, Glue. 
Raw Data file can be stored in S3 bucket and later processd to snowflake usng `Copy into` Command in staging table and later we can run the generated queries to create table and insert data into it.

## Conclusion
The Hospital Patient Data Processor is a robust and scalable tool designed to manage large volumes of patient data efficiently, ensuring data integrity. Feel free to ask enhancement to the current configuration.


## Contact
For any questions or suggestions, feel free to reach out to the project maintainer or @`sudhansu65r@gmail.com`



