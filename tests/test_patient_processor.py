import pytest
import pandas as pd
import sys
import os

# Add the parent directory of 'src' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.patient_processor import PatientProcessor


def setup_module(module):
    # Create necessary directories for test outputs
    if not os.path.exists("../output"):
        os.makedirs("../output")
    if not os.path.exists("../output/raw data"):
        os.makedirs("../output/raw data")
    if not os.path.exists("../output/sql_queries"):
        os.makedirs("../output/sql_queries")


def teardown_module(module):
    # Clean up test outputs after tests
    if os.path.exists("../output"):
        for root, dirs, files in os.walk("../output", topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir("../output")


def test_process_data():
    # Initialize the processor
    processor = PatientProcessor("../data/patient_data.txt", output_dir="../output")

    # Run the processing
    processor.process_data(current_date="20231010", days=30)

    # Check that raw data file exists
    raw_data_file = "../output/raw data/cleaned_patient_data.csv"
    assert os.path.exists(raw_data_file), "Raw data file does not exist."

    # Load the raw data and check columns
    raw_data = pd.read_csv(raw_data_file)
    expected_columns = [
        "Customer_Name",
        "Customer_Id",
        "Open_Date",
        "Last_Consulted_Date",
        "Vaccination_Id",
        "Dr_Name",
        "State",
        "Country",
        "DOB",
        "Is_Active",
        "Age",
        "Days_Since_Last_Consulted",
    ]
    assert (
        list(raw_data.columns) == expected_columns
    ), "Raw data columns do not match expected columns."

    # Check that country-wise files are created
    expected_countries = raw_data["Country"].unique()
    for country in expected_countries:
        country_file = f"../output/Table_{country}.csv"
        assert os.path.exists(
            country_file
        ), f"Country file {country_file} does not exist."

        # Load the country data and check columns
        country_data = pd.read_csv(country_file)
        expected_country_columns = [
            "Unique_ID",
            "Patient Name",
            "Vaccine Type",
            "Date of Birth",
            "Date of Vaccination",
            "Age",
            "Days_Since_Last_Consulted",
        ]
        assert (
            list(country_data.columns) == expected_country_columns
        ), f"Columns in {country_file} do not match expected columns."

        # Check that Unique_ID is monotonically increasing
        assert all(
            country_data["Unique_ID"] == range(1, len(country_data) + 1)
        ), f"Unique_ID in {country_file} is not monotonically increasing."

        # Check that Days_Since_Last_Consulted > 30
        assert all(
            country_data["Days_Since_Last_Consulted"] > 30
        ), f"Data in {country_file} contains records with Days_Since_Last_Consulted <= 30."


def test_generate_and_save_queries():
    # Initialize the processor
    processor = PatientProcessor("../data/patient_data.txt", output_dir="../output")

    # Process the data (which will also generate and save queries)
    processor.process_data(current_date="20231010", days=30)

    # Check that the SQL queries directory exists
    sql_queries_dir = "../output/sql_queries"
    assert os.path.exists(sql_queries_dir), "SQL queries directory does not exist."

    # Fetch unique countries from the data
    raw_data_file = "../output/raw data/cleaned_patient_data.csv"
    raw_data = pd.read_csv(raw_data_file)
    expected_countries = raw_data["Country"].unique()

    # Check that SQL files are created for each country
    for country in expected_countries:
        table_name = f"Table_{country}"
        query_file = os.path.join(sql_queries_dir, f"{table_name}_create_insert.sql")
        assert os.path.exists(query_file), f"SQL file {query_file} does not exist."

        # Load and check the content of the SQL file
        with open(query_file, "r") as f:
            query = f.read()
            assert (
                f"CREATE OR REPLACE TABLE {table_name}" in query
            ), f"CREATE TABLE statement missing in {query_file}."
            assert (
                f"INSERT INTO {table_name}" in query
            ), f"INSERT INTO statement missing in {query_file}."


if __name__ == "__main__":
    pytest.main()
