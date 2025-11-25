import pandas as pd
import os
import json

# --- 1. CONFIGURATION ---
# IMPORTANT: Using the filenames from your output
ENGLISH_FILE_USER = 'english_prompt (4).parquet'
MALAY_FILE_USER = 'malay_prompt (1).parquet'

# List of columns where the actual prompt text is likely stored:
# (Prioritize the columns that look like they contain the final content)
ENGLISH_TEXT_COLUMNS = ['Recommendation', 'Recommendation_', 'answer_queries', 'answer_queries_', 'input_fromuser', 'input_fromuser_']
MALAY_TEXT_COLUMNS = ['Cadangan', 'Cadangan_', 'pengesahan_khabarangin', 'pengesahan_khabarangin_', 'Input_penerima', 'Input_penerima_']
# ------------------------

def extract_and_print(filename, relevant_columns):
    if not os.path.exists(filename):
        print(f"ERROR: File not found: {filename}")
        print("Please ensure the file is in the same folder as this script.")
        return

    print(f"\n=============================================")
    print(f"--- START: {filename} ---")
    print(f"=============================================")
    
    try:
        # Read the parquet file
        df = pd.read_parquet(filename, engine='pyarrow')
        
        print("\n--- Available Columns (Headers) in this file ---")
        column_names = list(df.columns)
        print(column_names)
        
        # Iterate over the specific columns we think have the prompts
        for col_name in relevant_columns:
            if col_name not in df.columns:
                continue

            print(f"\n=============================================")
            print(f"--- CONTENT OF CRITICAL COLUMN: '{col_name}' ---")
            print(f"=============================================")
            
            content_found = False
            # Check the first 5 rows for non-empty text
            for index, text in enumerate(df[col_name].head(5)):
                # Check for non-missing and non-empty text
                if pd.notna(text) and str(text).strip() not in ('', ' '):
                    print(f"PROMPT/TEXT CHUNK (Row {index}):")
                    # Try to parse as JSON if it looks like structured data (often happens in parquet files)
                    if isinstance(text, str) and (text.startswith('{') or text.startswith('[')):
                        try:
                            parsed = json.loads(text)
                            # If it's a list, print the first item
                            if isinstance(parsed, list) and parsed:
                                print(parsed[0])
                            # If it's a dict, print a key that looks like the prompt
                            elif isinstance(parsed, dict):
                                for key in ['prompt', 'system', 'template']:
                                    if key in parsed:
                                        print(f"[{key.upper()} FOUND]: {parsed[key]}")
                                        break
                                else:
                                     print(text) # If no known key, print the raw text
                            else:
                                print(text)
                        except json.JSONDecodeError:
                            print(text)
                    else:
                        print(text)
                    
                    print("\n--- END OF CHUNK ---")
                    content_found = True
            
            if not content_found:
                print(f"No meaningful text found in column '{col_name}' in the first 5 rows.")
             
    except Exception as e:
        print(f"\nAN UNEXPECTED ERROR OCCURRED: {e}")
        print("Please review the error message.")

# Execute the extraction for both files
extract_and_print(ENGLISH_FILE_USER, ENGLISH_TEXT_COLUMNS)
extract_and_print(MALAY_FILE_USER, MALAY_TEXT_COLUMNS)

print("\n\n✅ Final extraction attempt complete. COPY THE ENTIRE OUTPUT above.")