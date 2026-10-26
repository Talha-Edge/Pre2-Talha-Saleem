from pathlib import Path
import pandas as pd

# Resolve CSV path relative to this script directory
csv_path = Path(__file__).parent / "articles_bulk.csv"

# Read the bulk articles CSV file
df = pd.read_csv(csv_path)

# Display the first 5 rows
print(df.head(5))
