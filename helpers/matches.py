import pandas as pd
from fuzzywuzzy import process


def padronize_data(column: pd.Series, limit=85):
    unique_names = column.unique()
    mapping = {}

    for name in unique_names:
        if name not in mapping:
            matches = process.extractBests(name, unique_names, score_cutoff=limit, limit=1000)
            if matches:
                for match in matches:
                    mapping[match[0]] = name
            else:
                mapping[name] = name

    column = column.map(mapping)
    return column


if __name__ == '__main__':
    data = {
        "nome": [
            "Medeiros Filho, Joel João",
            "Medeiros, Joel",
            "Joel Medeiros",
            "Maria Silva",
            "Silva, Maria",
            "M. Silva",
            "João Medeiros Filho",
            "Joel J. Medeiros",
            "Silva Maria",
            "Maria S.",
            "Joao Medeiros Filho"
        ]
    }

    df = pd.DataFrame(data)

    df["nome"] = padronize_data(df["nome"], limit=75)
    print('Is this the end,')