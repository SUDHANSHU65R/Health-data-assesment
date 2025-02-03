# Hospital Patient Data Processor

## Project Overview
The Hospital Patient Data Processor is a Python-based ETL (Extract, Transform, Load) tool designed to handle patient data for a multi-specialty hospital chain. This tool processes patient data files, extracts relevant information, transforms it according to specified rules, and loads it into country-specific output files. Additionally, it generates SQL table creation queries for each country-specific table.

## Features
- **Data Extraction:** Reads patient data from a specified file.
- **Data Transformation:** Processes data to include age and days since the last consultation, filters records, and handles missing values.
- **Data Loading:** Splits data into country-specific CSV files with required columns and monotonically increasing `Unique_ID`.
- **Validation:** Ensures data integrity by dropping records with null values in mandatory fields.
- **SQL Queries:** Generates SQL queries for creating tables for each country-specific dataset.
