import pytest
import sys
import os

# Add the parent directory of 'src' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.patient_processor import PatientProcessor
import pandas as pd
from datetime import datetime

def setup_module(module):
    # Create an output directory for test outputs
    if not os.path.exists('../output'):
        os.makedirs('../output')

def teardown_module(module):
    # Clean up test outputs after tests
    if os.path.exists('../output'):
        for f in os.listdir('../output'):
            os.remove(os.path.join('../output', f))
        os.rmdir('../output')

def test_process_data():
    processor = PatientProcessor('../data/patient_data.txt', output_dir='../output')
    processor.process_data(current_date='20231010')
    print("Columns in processor.data:", processor.data.columns)  # Print for debugging
    assert 'IND' in processor.country_data
    assert not processor.data.empty
    assert all(col in processor.data.columns for col in [
        'Customer_Name', 'Vaccination_Id', 'DOB', 'Last_Consulted_Date', 'Age', 'Days_Since_Last_Consulted'
    ])
    for country in processor.country_data.keys():
        df = processor.country_data[country]
        print(f"Columns in {country} DataFrame:", df.columns)  # Print for debugging
        assert all(col in df.columns for col in [
            'Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date',    
       'Vaccination_Id', 'Dr_Name', 'State', 'Country', 'DOB', 'Is_Active',
       'Age', 'Days_Since_Last_Consulted'
        ])

def test_output_files():
    processor = PatientProcessor('../data/patient_data.txt', output_dir='../output')
    processor.process_data(current_date='20231010')
    output_files = os.listdir('../output')
    expected_files = [f"Table_{country}.csv" for country in processor.country_data.keys()]
    for expected_file in expected_files:
        assert expected_file in output_files
        # Load the output CSV and check columns
        df = pd.read_csv(os.path.join('../output', expected_file))
        expected_columns = ['Unique_ID', 'Patient Name', 'Vaccine Type', 'Date of Birth', 'Date of Vaccination', 'Age', 'Days_Since_Last_Consulted']
        assert list(df.columns) == expected_columns
        # Check for monotonically increasing Unique_ID
        assert all(df['Unique_ID'] == range(1, len(df) + 1))

