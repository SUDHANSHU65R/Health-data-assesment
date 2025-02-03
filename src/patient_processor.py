import pandas as pd
from datetime import datetime
import os


class PatientProcessor:
    def __init__(self, file_path, output_dir="output", chunk_size=1000000):
        self.file_path = file_path
        self.chunk_size = chunk_size  # For processing large files
        self.output_dir = output_dir

        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Initialize data attributes
        self.data = None
        self.country_data = {}

    def process_data(self, current_date=None, days=30):
        # Set current date
        if current_date is None:
            current_date = datetime.now().strftime("%Y%m%d")

        current_date = datetime.strptime(current_date, "%Y%m%d")
        processed_chunks = []

        # Define column names
        columns = [
            "Record_Type",
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
        ]

        # Define data types for efficient memory usage
        dtypes = {
            "Record_Type": "category",
            "Customer_Name": "string",
            "Customer_Id": "Int64",
            "Open_Date": "string",
            "Last_Consulted_Date": "string",
            "Vaccination_Id": "string",
            "Dr_Name": "string",
            "State": "string",
            "Country": "string",
            "DOB": "string",
            "Is_Active": "category",
        }

        # Read and process data in chunks
        for chunk in pd.read_csv(
            self.file_path,
            delimiter="|",
            skiprows=1,
            header=None,
            names=columns,
            dtype=dtypes,
            chunksize=self.chunk_size,
        ):

            # Filter 'D' records
            chunk = chunk[chunk["Record_Type"] == "D"].drop(columns=["Record_Type"])

            # Data type conversions with error handling
            chunk["Customer_Id"] = pd.to_numeric(chunk["Customer_Id"], errors="coerce")
            mandatory_fields = ["Customer_Name", "Customer_Id", "Open_Date"]
            # Drop rows with null values in mandatory fields
            chunk.dropna(subset=mandatory_fields, inplace=True)

            chunk["Open_Date"] = pd.to_datetime(
                chunk["Open_Date"], format="%Y%m%d", errors="coerce"
            )
            chunk["Last_Consulted_Date"] = pd.to_datetime(
                chunk["Last_Consulted_Date"], format="%Y%m%d", errors="coerce"
            )
            chunk["DOB"] = pd.to_datetime(
                chunk["DOB"], format="%d%m%Y", errors="coerce"
            )

            # Check if DOB conversion was successful
            if chunk["DOB"].isna().any():
                print(
                    "Warning: Some DOB values could not be converted. They will be dropped."
                )
                chunk = chunk.dropna(subset=["DOB"])

            # Calculate Age
            chunk["Age"] = current_date.year - chunk["DOB"].dt.year

            # Calculate Days Since Last Consulted
            chunk["Days_Since_Last_Consulted"] = (
                current_date - chunk["Last_Consulted_Date"]
            ).dt.days

            # Append to the list of processed chunks
            processed_chunks.append(chunk)

        # Concatenate all processed chunks
        if processed_chunks:
            self.data = pd.concat(processed_chunks)
        else:
            self.data = pd.DataFrame(columns=columns)

        # Handle customer transfers by keeping the latest record
        self.data.sort_values(
            by=["Customer_Id", "Last_Consulted_Date"],
            ascending=[True, False],
            inplace=True,
        )
        self.data.drop_duplicates(subset="Customer_Id", keep="first", inplace=True)

        # Save the final cleaned data to a single CSV file
        self.save_cleaned_data()

        # Filter data with Days_Since_Last_Consulted > 30
        self.data = self.data[self.data["Days_Since_Last_Consulted"] > days]

        # Save country-wise CSV outputs
        self.save_country_wise_data()

        # Generate and store country-wise create table queries
        self.generate_and_save_queries()

    def save_cleaned_data(self):
        # Create 'raw data' directory inside the output directory
        raw_data_dir = os.path.join(self.output_dir, "raw data")
        if not os.path.exists(raw_data_dir):
            os.makedirs(raw_data_dir)

        # Save the cleaned data with all columns in 'raw data' folder
        filename = os.path.join(raw_data_dir, "cleaned_patient_data.csv")
        self.data.to_csv(filename, index=False)
        print(f"Cleaned data saved to '{filename}' with all original columns.")

    def save_country_wise_data(self):
        grouped = self.data.groupby("Country")
        for country, group in grouped:
            # Select only the required columns and rename them
            output_group = group[
                [
                    "Customer_Name",
                    "Vaccination_Id",
                    "DOB",
                    "Last_Consulted_Date",
                    "Age",
                    "Days_Since_Last_Consulted",
                ]
            ].copy()
            output_group.rename(
                columns={
                    "Customer_Name": "Patient Name",
                    "Vaccination_Id": "Vaccine Type",
                    "DOB": "Date of Birth",
                    "Last_Consulted_Date": "Date of Vaccination",
                    "Age": "Age",
                    "Days_Since_Last_Consulted": "Days_Since_Last_Consulted",
                },
                inplace=True,
            )

            # Add Unique_ID column with monotonically increasing values
            output_group.insert(0, "Unique_ID", range(1, len(output_group) + 1))

            # Save to CSV file
            filename = os.path.join(self.output_dir, f"Table_{country}.csv")
            output_group.to_csv(filename, index=False)
            print(
                f"Data for country '{country}' saved to '{filename}' with specified columns."
            )

    def generate_and_save_queries(self):
        unique_countries = self.data["Country"].unique()
        sql_queries_dir = os.path.join(self.output_dir, 'sql_queries')
        if not os.path.exists(sql_queries_dir):
            os.makedirs(sql_queries_dir)


        for country in unique_countries:
            table_name = f"Table_{country}"
            query = f"""
            -- Create table for {country}
            CREATE OR REPLACE TABLE {table_name} (
                Unique_ID INT PRIMARY KEY,
                Patient_Name VARCHAR(255),
                Vaccine_Type CHAR(5),
                Date_of_Birth DATE,
                Date_of_Vaccination DATE,
                Age INT,
                Days_Since_Last_Consulted INT
            );

            -- Insert filtered data into the table
            INSERT INTO {table_name} (
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
                Country = '{country}'
                AND DATEDIFF(DAY, TO_DATE(Last_Consulted_Date, 'YYYYMMDD'), CURRENT_DATE) > 30;
            """

            filename = os.path.join(sql_queries_dir, f"{table_name}_create_insert.sql")
            with open(filename, 'w') as f:
                f.write(query)

            print(f"Create and insert query for '{table_name}' saved to '{filename}'.")
