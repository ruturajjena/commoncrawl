import pandas as pd
from tqdm import tqdm  # Import tqdm for the progress bar

# Load NAICS and SIC code data
naics_df = pd.read_excel('6-digit_2022_naics_Codes.xlsx')
print(naics_df.head())
sic_df = pd.read_excel('sic_8_digit_codes.xls')
print(sic_df.head())

# Function to classify websites based on keyword matching from NAICS and SIC codes
def classify_website(url):
    # Extract relevant keywords from the URL (split by '.' in this case)
    keywords = url.split('.')
    
    try:
        # Try to match keywords with NAICS and SIC codes
        matched_naics = naics_df[naics_df['2022 NAICS Title'].str.contains('|'.join(keywords), case=False, na=False)]
        matched_sic = sic_df[sic_df['Description'].str.contains('|'.join(keywords), case=False, na=False)]
        
        # If matched, return the first match from both NAICS and SIC
        primary_naics_code = matched_naics.iloc[0]['2022 NAICS Code'] if not matched_naics.empty else 'Unknown'
        primary_sic_code = matched_sic.iloc[0]['SIC 8-digit'] if not matched_sic.empty else 'Unknown'
        
        # Secondary classification: search for codes related to the primary match
        secondary_naics_code = 'Unknown'
        secondary_sic_code = 'Unknown'

        if not matched_naics.empty:
            primary_naics_description = matched_naics.iloc[0]['2022 NAICS Title']
            # Find a secondary NAICS code that is related (this can be expanded further)
            secondary_naics = naics_df[naics_df['2022 NAICS Title'].str.contains(primary_naics_description.split()[0], case=False, na=False)]
            if not secondary_naics.empty:
                secondary_naics_code = secondary_naics.iloc[1]['2022 NAICS Code'] if len(secondary_naics) > 1 else 'Unknown'
        
        if not matched_sic.empty:
            primary_sic_description = matched_sic.iloc[0]['Description']
            # Find a secondary SIC code that is related (this can be expanded further)
            secondary_sic = sic_df[sic_df['Description'].str.contains(primary_sic_description.split()[0], case=False, na=False)]
            if not secondary_sic.empty:
                secondary_sic_code = secondary_sic.iloc[1]['SIC 8-digit'] if len(secondary_sic) > 1 else 'Unknown'
        
        return primary_naics_code, primary_sic_code, secondary_naics_code, secondary_sic_code
    
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return 'Unknown', 'Unknown', 'Unknown', 'Unknown'


# Function to process the data in batches with a progress bar
def process_in_batches(input_file, batch_size=10000, output_file='classified_websites_with_secondary.csv'):
    # Read the data in chunks
    chunks = pd.read_csv(input_file, chunksize=batch_size)
    
    # Create an empty DataFrame to store results
    all_results = pd.DataFrame(columns=['url_host_name', 'naics_code', 'sic_code', 'secondary_naics_code', 'secondary_sic_code'])
    
    # Loop over each chunk and apply the classification
    for chunk in tqdm(chunks, desc="Processing batches", unit="batch"):
        # Apply the classification function to each batch
        chunk['naics_code'], chunk['sic_code'], chunk['secondary_naics_code'], chunk['secondary_sic_code'] = zip(*chunk['url_host_name'].apply(classify_website))
        
        # Append the processed chunk to the results
        all_results = pd.concat([all_results, chunk], ignore_index=True)
        
        # Optionally, save the result after each batch to avoid data loss
        all_results.to_csv(output_file, index=False)
        print(f"Processed {len(chunk)} records, total so far: {len(all_results)}")
    
    # Return the final classified DataFrame (optional)
    return all_results

# Call the function to process the file
output_df = process_in_batches('run-1750350462646-part-r-00003.csv')

# Optionally, save the final result to disk
output_df.to_csv('final_classified_websites_with_secondary.csv', index=False)
