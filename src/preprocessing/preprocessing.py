import pandas as pd
import re
import os

# Incorporating etnltk
try:
    from etnltk import Amharic
    # You might also want these for general cleaning if etnltk.Amharic doesn't do it internally
    # from etnltk.common.preprocessing import (
    #     remove_whitespaces, remove_special_characters, remove_tags,
    #     remove_emojis, remove_links, remove_email
    # )
    # from etnltk.common.ethiopic import remove_ethiopic_punctuation
except ImportError:
    print("etnltk modules are not installed. Please install it using 'pip install etnltk'.")
    exit()

# Amharic preprocessing function
def preprocess_amharic_text(text):
    # CRITICAL: Convert to string and strip whitespace.
    # df_raw['Message'].fillna('') in main() already ensures 'text' is a string,
    # but 'strip()' is essential to handle empty strings or strings with only whitespace.
    cleaned_text = str(text).strip()

    # CRITICAL: Check if the string is empty *after* stripping.
    if not cleaned_text:
        return [] # Return an empty list for effectively empty messages

    try:
        doc = Amharic(cleaned_text) # Pass the cleaned, non-empty string to Amharic()
        preprocessed_tokens = doc.words
        return preprocessed_tokens
    except ValueError as e:
        # This specific ValueError indicates etnltk didn't like the input.
        # Print a warning and return empty to keep processing.
        print(f"Warning: etnltk ValueError for text (after strip): '{cleaned_text[:50]}...'. Original: '{str(text)[:50]}...'. Error: {e}")
        return []
    except Exception as e:
        # Catch any other unexpected errors during Amharic processing.
        print(f"Warning: Unexpected error in Amharic processing for text: '{cleaned_text[:50]}...'. Error: {e}")
        return []

def main():
    # ... (rest of your main function remains the same, as it's correct) ...
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))

    raw_data_dir = os.path.join(project_root, 'data', 'raw')
    processed_data_dir = os.path.join(project_root, 'data', 'processed')

    input_csv_path = os.path.join(raw_data_dir, 'telegram_data.csv')
    output_csv_path = os.path.join(processed_data_dir, 'preprocessed_telegram_data.csv')

    print(f"Starting Amharic-only data preprocessing...")
    print(f"Input CSV: {input_csv_path}")
    print(f"Output CSV: {output_csv_path}")

    os.makedirs(processed_data_dir, exist_ok=True)

    try:
        df_raw = pd.read_csv(input_csv_path)
        print(f"Successfully loaded {len(df_raw)} rows from '{input_csv_path}'.")
    except FileNotFoundError:
        print(f"Error: Input file '{input_csv_path}' not found. Please ensure your raw data is in 'data/raw/'.")
        print("Expected structure: your_project_root/data/raw/telegram_data.csv")
        return
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    if 'Message' not in df_raw.columns:
        print("Error: 'Message' column not found in the input CSV. Please check your CSV header.")
        return

    # This line is correct and crucial for robustness
    df_raw= df_raw.dropna(subset=['Message'])  # Drop rows where 'Message' is NaN or empty
    print("Drop any row that contains NaN messages for robust preprocessing.")

    print("Applying Amharic text preprocessing to 'Message' column...")

    df_raw['Preprocessed_Tokens_List'] = df_raw['Message'].apply(preprocess_amharic_text)

    df_raw['Detected_Language'] = 'am'

    df_raw['Preprocessed_Message_Text'] = df_raw['Preprocessed_Tokens_List'].apply(
        lambda x: ' '.join(x) if isinstance(x, list) else ''
    )

    df_processed = df_raw.copy()

    print("Amharic text preprocessing complete.")

    try:
        df_processed.to_csv(output_csv_path, index=False, encoding='utf-8')
        print(f"Preprocessed Amharic data saved successfully to '{output_csv_path}'.")
        print(f"Example of preprocessed data (first 5 rows, Message, Detected_Language, Preprocessed_Message_Text):")
        print(df_processed[['Message', 'Detected_Language', 'Preprocessed_Message_Text']].head())
    except Exception as e:
        print(f"Error saving preprocessed data: {e}")

if __name__ == "__main__":
    main()