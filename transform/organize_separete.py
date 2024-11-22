import re

import numpy as np
import pandas as pd
from fuzzywuzzy import process
from .verificator import text_is_written_in_language

def separate_languages(row):
    texts = row if isinstance(row, np.ndarray) else [row]
    pt_texts = [text for text in texts if text_is_written_in_language(text, 'pt')]
    en_texts = [text for text in texts if text_is_written_in_language(text, 'en')]

    return pd.Series({
        'coluna_pt': pt_texts if pt_texts else None,
        'coluna_en': en_texts if en_texts else None
    })


def normalize_individual_names(names_list, threshold=65):
    unique_names = []
    mapping = {}


    for name in names_list:
        if type(name) != str:
            name = name[0]

        best_name = process.extractOne(name, unique_names, score_cutoff=threshold)
        if best_name:
            mapping[name] = best_name[0]
        else:
            unique_names.append(name)
            mapping[name] = name

    return mapping

def convert_to_json_serializable(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def transform_to_list(text):

    if isinstance(text, np.ndarray):
        return text

    if pd.isna(text) or not isinstance(text, str):
        return text

    delimiters = [',', ';', '.']

    if any(re.search(fr'{delimiter}', text) for delimiter in delimiters):

        for delimiter in delimiters:
            text = re.sub(fr'\s*{delimiter}\s*', ',', text)

        return [item.strip() for item in text.split(',') if item.strip()]

    return text
