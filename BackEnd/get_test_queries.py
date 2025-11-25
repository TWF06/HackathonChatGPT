import pandas as pd
import os

# --- Configuration (Must match your file names and columns) ---
ENGLISH_FILE = 'english_prompt (4).parquet'
MALAY_FILE = 'malay_prompt (1).parquet'

# These column names are based on your rag_service.py logic
ENGLISH_QUERY_COL = 'input_fromuser'
MALAY_QUERY_COL = 'Input_penerima'
# -----------------------------------------------------------

def display_queries(filename, query_col, language):
    if not os.path.exists(filename):
        print(f"❌ ERROR: File not found: {filename}")
        return

    print(f"\n=======================================================")
    print(f"✅ Sample {language} Queries from {filename}:")
    print(f"=======================================================")
    
    try:
        # Read the file and display the first 5 unique queries
        df = pd.read_parquet(filename, columns=[query_col], engine='pyarrow')
        
        # Display the first 5 unique queries found in the column
        unique_queries = df[query_col].dropna().unique()[:5]

        for i, query in enumerate(unique_queries):
            print(f"[{i+1}] {query.strip()}")
            
        print("\nINSTRUCTIONS: Copy one of the above queries EXACTLY for your API test.")

    except Exception as e:
        print(f"❌ An error occurred reading {filename}: {e}")

# Run the display function for both files
display_queries(ENGLISH_FILE, ENGLISH_QUERY_COL, "ENGLISH")
display_queries(MALAY_FILE, MALAY_QUERY_COL, "MALAY")