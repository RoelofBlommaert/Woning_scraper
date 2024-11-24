import pandas as pd

def remove_rows_without_http(input_file, output_file):
    # Read the file into a DataFrame
    df = pd.read_csv(input_file)

    # Check if the DataFrame is empty
    if df.empty:
        print("No data in the file.")
        return

    # Remove rows where 'url' does not contain 'http'
    # Assuming the 'url' column exists in your data
    df_clean = df[df['url'].str.contains('http', na=False)]

    # Save the cleaned DataFrame back to the file
    df_clean.to_csv(output_file, index=False)

def dedupe_file(input_file, output_file):
    # Step 1: Remove rows where 'url' does not contain 'http'
    temp_file = 'temp_with_http.csv'
    remove_rows_without_http(input_file, temp_file)

    # Step 2: Read the cleaned CSV file
    df = pd.read_csv(temp_file)

    # Step 3: Drop duplicates based on a unique identifier, e.g., 'price' + 'postal_code' + 'square_meters'
    df['unique_id'] = df['price'].astype(str) + '-' + df['postal_code'].astype(str) + '-' + df['square_meters'].astype(str)
    df_dedup = df.drop_duplicates(subset='unique_id')

    # Step 4: Save the deduplicated result to the output file
    df_dedup.to_csv(output_file, index=False)

# Example usage:
input_csv_file = 'all_scraped.csv'
output_csv_file = 'all_scraped_deduped.csv'

# Run the deduplication
dedupe_file(input_csv_file, output_csv_file)
