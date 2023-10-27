import pandas as pd 
import re

cleaned_data = pd.read_json("data/Processed/processed_sample.json")

ppm = cleaned_data['price'] / cleaned_data['living_area']
if ppm.between(500, 15000).all():
    print("Condition 1 (price_per_square_meter) is satisfied.")
else:
    print("Condition 1 (price_per_square_meter) is NOT satisfied.")


if cleaned_data['property_type'].isin(['apartment', 'house']).all():
    print("Condition 2 (property_type) is satisfied.")
else:
    print("Condition 2 (property_type) is NOT satisfied.")

date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
if cleaned_data['scraping_date'].astype(str).str.match(date_pattern).all():
    print("Condition 3 (scraping_date format) is satisfied.")
else:
    print("Condition 3 (scraping_date format) is NOT satisfied.")

