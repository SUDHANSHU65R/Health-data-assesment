import pandas as pd
from datetime import datetime
import os


class PatientProcessor:
    def __init__(self, file_path, output_dir="../output", chunk_size=1000000):
        self.file_path = file_path
        self.chunk_size = chunk_size  # For processing large files
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.data = None
        self.country_data = {}

    def process_data(self, current_date=None, days=30):
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
            "Customer_Id": "int64",
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

            # Calculate Age
            chunk["Age"] = current_date.year - chunk["DOB"].dt.year

            # Calculate Days Since Last Consulted
            chunk["Days_Since_Last_Consulted"] = (
                current_date - chunk["Last_Consulted_Date"]
            ).dt.days

            # Filter records where Days_Since_Last_Consulted > 30
            chunk = chunk[chunk["Days_Since_Last_Consulted"] > days]

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

        # Print columns for debugging
        print(
            "Columns in processor.data before splitting by country:", self.data.columns
        )

        # Split data by country and output to CSV files
        self.split_by_country_and_save()

    def split_by_country_and_save(self):
        grouped = self.data.groupby("Country")
        for country, group in grouped:
            self.country_data[country] = group.copy()

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


