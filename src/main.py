import sys
import os

# Add the parent directory of 'src' to the Python path
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from patient_processor import PatientProcessor

if __name__ == "__main__":
    processor = PatientProcessor("../data/patient_data.txt", output_dir="../output")
    processor.process_data(current_date="20231010", days=30)

