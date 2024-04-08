import pandas as pd
from bs4 import BeautifulSoup
import json

# Function to parse the HTML content of the 'reviewText' column
def parse_html(review):
    if review:
        soup = BeautifulSoup(review, 'html.parser')
        return soup.get_text()
    else:
        return ''

# Function to preprocess the metadata
def preprocess_metadata(metadata_df):
    metadata_df.drop_duplicates(subset='product_id', inplace=True)  # Remove duplicate products
    metadata_df.reset_index(drop=True, inplace=True)

# Choose the product of interest
product_name = 'headphones'

# Load metadata from JSON file into a dataframe
with open('meta_Electronics.json', 'r') as file:
    metadata_dict = json.load(file)

metadata_df = pd.DataFrame(metadata_dict)

# Preprocess metadata
preprocess_metadata(metadata_df)

# Load Amazon reviews data into a dataframe
reviews_df = pd.read_json('Electronics_5.json', lines=True)

# Create an empty dataframe to store the processed data
processed_data = pd.DataFrame()

# Load JSON data in chunks
chunk_size = 25  # 25% of the data
total_chunks = 4  # 100% of the data

for i in range(total_chunks):
    # Calculate start and end indices for the chunk
    start_index = int(i * len(metadata_df) / total_chunks)
    end_index = int((i + 1) * len(metadata_df) / total_chunks)

    # Filter metadata dataframe for the current chunk
    chunk_metadata = metadata_df.iloc[start_index:end_index]

    # Filter Amazon reviews data for the product of interest and merge with metadata
    chunk_df = pd.DataFrame()
    for index, row in chunk_metadata.iterrows():
        product_id = row['product_id']
        product_reviews = reviews_df[reviews_df['product_id'] == product_id]
        chunk_df = pd.concat([chunk_df, product_reviews])

    # Parse HTML content in the 'reviewText' column
    chunk_df['reviewText'] = chunk_df['reviewText'].apply(parse_html)

    # Append the chunk to the processed data
    processed_data = pd.concat([processed_data, chunk_df])

# Handle missing values and duplicates
processed_data.dropna(inplace=True)  # Remove rows with missing values
processed_data.drop_duplicates(inplace=True)  # Remove duplicate rows

# Reset index after preprocessing
processed_data.reset_index(drop=True, inplace=True)

# Report the total number of rows after preprocessing
total_rows_after_preprocessing = len(processed_data)
print(f"Total number of rows after preprocessing: {total_rows_after_preprocessing}")

# Save the processed data as CSV
processed_data.to_csv(f'{product_name}_processed_data.csv', index=False)
