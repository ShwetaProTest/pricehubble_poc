# This script cleans data from a variety of formats (JSON, CSV, and Avro) by validating and cleaning the following columns:
# * id
# * price
# * living_area
# * property_type
# * scraping_date
# The script also filters out offers with invalid or incomplete data, and only keeps offers with a price per square meter between 500 and 15000.
# Created_at:26-10-2023

import os
import pandas as pd
import json
import logging
import re
from pathlib import Path

input_path = os.environ.get('INPUT_PATH')
output_path = os.environ.get('OUTPUT_PATH')
log_path = os.environ.get('LOG_PATH')

# Constants
LOG_FILE = log_path
VALID_PROPERTY_TYPES = ['apartment', 'house']
DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')

if not input_path:
    raise ValueError("INPUT_PATH is not set in config or environment variable.")
if not output_path:
    raise ValueError("OUTPUT_PATH is not set in config or environment variable.")
if not log_path:
    raise ValueError("LOG_PATH is not set in config or environment variable.")

logging.basicConfig(filename=LOG_FILE, level=logging.ERROR)

class DataProcessor:
    def __init__(self, input_path):
        self.validate_file_path(input_path)
        self.data = self.load_data(input_path)

    @staticmethod
    def validate_file_path(path):
        """Check if the input file exists. If not, raise an exception.
        Args:
        path: The path to the input file.
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"The provided file path does not exist: {path}")

    def load_data(self, path):
        """Loads the data from the input file.

        Args:
            path: The path to the input file.

        Returns:
            A Pandas DataFrame containing the loaded data.
        """
        file_ext = Path(path).suffix.lower()

        if file_ext == ".json":
            with open(path, "r", encoding='utf-8') as file:
                content = file.read()
                try:
                    data_list = json.loads(json.loads(content))
                except json.JSONDecodeError:
                    # If double-serialization fails, attempt to load as regular JSON
                    try:
                        data_list = json.loads(content)
                    except:
                        raise ValueError("Unable to parse the provided JSON data.")
                
                # Ensure that the loaded data is a list before creating a DataFrame
                if not isinstance(data_list, list):
                    raise ValueError("JSON content does not represent a list structure.")
            
            return pd.DataFrame(data_list).drop(columns='municipality', errors='ignore')
        elif file_ext == ".csv":
            return pd.read_csv(path)
        elif file_ext == ".avro":
            return pd.read_avro(path)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

    def log_errors(self, condition, message):
        """Utility function to log error messages for rows that satisfy a given condition.

        Args:
            condition: A Pandas Series containing the condition to check.
            message: The error message to log if the condition is satisfied.
        """
        if condition.any():
            indices = ", ".join(condition.index.astype(str))
            logging.error(f"{message} Indices: {indices}")

    def validate_and_clean(self):
        """Validates and cleans the data."""
        self.validate_id_column()
        self.validate_clean_price()
        self.validate_living_area()
        self.filter_by_price_per_sqm()
        self.validate_property_type()
        self.validate_scraping_date()

    def validate_id_column(self):
        """Validates the id column.Any rows with missing or invalid IDs will be removed."""
        self.log_errors(self.data['id'].isnull(), "Null IDs found.")
        self.log_errors(~self.data['id'].astype(str).str.isnumeric(), "ID is not a string or numeric.")

    def validate_clean_price(self):
        """Validates and cleans the price column.Any rows with invalid or missing prices will be removed."""

        if 'raw_price' in self.data.columns:
            self.data['price'] = self.data['raw_price'].str.extract('(\d+\.?\d*)', expand=False).astype(float)
            self.log_errors(self.data['price'].isnull(), "Invalid or null prices found.")
            self.data.drop(columns=['raw_price'], inplace=True)
        else:
            logging.error("raw_price column missing.")

    def filter_by_price_per_sqm(self):
        """Filter offers based on price per square meter criteria.
        Only offers with a price per square meter between 500 and 15000 will be kept."""
        valid_offers = (self.data['price'] / self.data['living_area']).between(500, 15000)
        self.data = self.data[valid_offers]

    def validate_living_area(self):
        """Validates and cleans the living_area column.
        Any rows with invalid or missing living areas will be removed."""
        invalid_area = ~self.data['living_area'].between(10, 500)
        self.log_errors(invalid_area, "Invalid living_area values.")
        self.data = self.data[~invalid_area]

    def validate_property_type(self):
        """Validates and cleans the property_type column.Any rows with invalid property types will be removed."""
        valid_types = VALID_PROPERTY_TYPES
        invalid_types = ~self.data['property_type'].isin(valid_types)
        self.log_errors(invalid_types, "Invalid property types. Only 'apartment' or 'house' are allowed.")

    def validate_scraping_date(self):
        """Validates and cleans the scraping_date column.Any rows with invalid scraping dates will be removed."""
        date_pattern = DATE_PATTERN
        invalid_dates = ~self.data['scraping_date'].astype(str).str.match(date_pattern)
        self.log_errors(invalid_dates, "Invalid date formats in scraping_date. Expected format 'YYYY-MM-DD'.")

    def save_processed_data(self, output_path):
        """Save the cleaned data. If the destination folder doesn't exist, create it."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        self.data.to_json(output_path, orient='records')

if __name__ == "__main__":
    try:
        processor = DataProcessor(input_path)
        processor.validate_and_clean()
        processor.save_processed_data(output_path)
    except Exception as e:
        print(e)