import pandas as pd
import re
import os

# Incorporating etnltk
try:
    from etnltk import Amharic
    from etnltk.common.preprocessing  import (remove_whitespaces, remove_special_characters, remove_tags,
                                              remove_emojis, remove_links, remove_email,remove_english_chars,remove_arabic_chars,
                                              remove_chinese_chars,remove_special_characters)
    
    from etnltk.common.ethiopic import (remove_ethiopic_punctuation,remove_ethiopic_digits,remove_ethiopic_digits)
    from etnltk.lang.am import clean_amharic
    from etnltk.lang.am import normalize
    from etnltk.tokenize.am import word_tokenize

except ImportError:
    print("etnltk modules are not installed. Please install it using 'pip install etnltk'.")
    exit()

# Amharic preprocessing function
def preprocess_amharic_text(text):
    cleaned_text = str(text).strip()

    # CRITICAL: Check if the string is empty *after* stripping.
    if not cleaned_text:
        return [] # Return an empty list for effectively empty messages

    try:
        custom_pipeline=[remove_whitespaces, remove_special_characters, remove_tags,
                   remove_emojis, remove_links, remove_email, remove_ethiopic_digits,
                   remove_ethiopic_punctuation,remove_english_chars,remove_arabic_chars,remove_chinese_chars,remove_special_characters]

        doc = clean_amharic(cleaned_text,pipeline=custom_pipeline)
        Message_doc=normalize(doc) # avoided using Amharic to extract digits
        preprocessed_tokens = word_tokenize(Message_doc,return_word=False)
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