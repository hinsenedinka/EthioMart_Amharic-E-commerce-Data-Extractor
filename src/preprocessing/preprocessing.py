import pandas as pd
import re
import unicodedata
import os
from langdetect import detect, DetectorFactory # For language detection
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

# Incorporating etnltk
try:
    from etnltk.common.preprocessing  import (remove_whitespaces,
                                              remove_special_characters,
                       remove_tags, 
                       remove_emojis, 
                       remove_links, 
                       remove_email)
    from etnltk.common.ethiopic import remove_ethiopic_punctuation
    from etnltk.lang.am import  (normalize,clean_amharic)
    print("etnltk preprocessing modules successfully installed.")
    from etnltk.tokenize.am import word_tokenize as amharic_word_tokenize
    print("etnltk tokenizer successfully installed.")

except ImportError:
    print("etnltk modules are not installed. Please install it using 'pip install etnltk'.")
    exit()

# Amharic preprocessing function

def preprocess_amharic_text(text):
    text=text.str
    # Define a custom preprocessor pipeline
    customer_pipeline=[ remove_whitespaces, remove_special_characters,
                       remove_tags, remove_emojis, remove_links, remove_email, remove_ethiopic_punctuation, normalize]
    
    doc=clean_amharic(text, customer_pipeline=customer_pipeline,keep_abbrev=False)
    doc=amharic_word_tokenize(doc)

    return doc

# English preprocessing function
def preprocess_english_text(text):
    # Normalize text
    text = text.str.lower()
    text = text.str.replace(r'\d+', '', regex=True)  # Remove digits
    text = text.str.replace(r'\s+', ' ', regex=True).str.strip()  # Remove extra spaces

    # Remove special characters
    text = text.str.replace(r'[^\w\s]', '', regex=True)

    # Tokenization
    tokens = text.apply(word_tokenize)

    # Stop words removal
    stop_words = set(stopwords.words('english'))
    tokens = tokens.apply(lambda x: [word for word in x if word not in stop_words])

    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    tokens = tokens.apply(lambda x: [lemmatizer.lemmatize(word) for word in x])

    return tokens

# Language detection function
