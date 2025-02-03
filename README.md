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
hospital_assessment/  
├── data/  
│   └── patient_data.txt  
├── output/  
│   └── [Generated CSV Files]  
├── src/  
│   └── patient_processor.py  
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

