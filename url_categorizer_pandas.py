
import pandas as pd
import re
from tqdm import tqdm

# Load your files
urls_df = pd.read_csv(r"C:\Users\rutur\Desktop\commoncrawl\new_data\may\run-1750350462646-part-r-00000.csv", header=None, names=["URL"])
categories_df = pd.read_csv("4-6-8 Sics and NASIC and Main and Sub cats 6.11.25.csv")

# Clean and prepare URL data
urls_df = urls_df.iloc[1:].copy()
urls_df["url_lower"] = urls_df["URL"].str.lower()

# Build keyword mapping
from collections import defaultdict

category_keywords = defaultdict(dict)

for _, row in categories_df.iterrows():
    main_cat = str(row["category_name"]).strip()
    sub_cat = str(row["sub_subcategory_name"]).strip()

    combined_text = " ".join([
        str(row["sic_description"]),
        str(row["sic_6_digit_description"]),
        str(row["8_digit_Description"]),
        str(row["naics_description"])
    ]).lower()

    keywords = set(re.findall(r'\b\w{4,}\b', combined_text))
    category_keywords[(main_cat, sub_cat)] = keywords

# Compile regex map for performance
regex_map = []
for (main_cat, sub_cat), keywords in category_keywords.items():
    if keywords:
        pattern = r"|".join([re.escape(kw) for kw in keywords if len(kw) > 2])
        try:
            compiled = re.compile(pattern)
            regex_map.append((compiled, main_cat, sub_cat))
        except re.error:
            continue

# Batch process and match
final_results = []
batch_size = 100000

for start in tqdm(range(0, len(urls_df), batch_size), desc="Processing URL batches"):
    batch = urls_df.iloc[start:start + batch_size]
    for idx, row in batch.iterrows():
        url = row["url_lower"]
        for regex, main_cat, sub_cat in regex_map:
            if regex.search(url):
                final_results.append({
                    "URL": row["URL"],
                    "Main Category": main_cat,
                    "Subcategory": sub_cat
                })
                break

# Save results
final_df = pd.DataFrame(final_results)
final_df.to_csv("URL_Categorized_1M_Output.csv", index=False)
print("✅ Done! Results saved to URL_Categorized_1M_Output.csv")
