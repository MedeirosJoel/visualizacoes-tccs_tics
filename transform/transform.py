import json

import numpy as np
import pandas as pd


from .organize_separete import separate_languages, normalize_individual_names, transform_to_list
from .clean_data import obtain_first_number_in_text


columns_for_drop = ['dc.subject.classification',
                    'dc.title.alternative', # não possui dados o suficiente para ser relevante
                    'dc.description', # tipo de documento, todos são TCCs do TIC
                    'dc.publisher', # local da publicação, todos são em Araranguá
                    'dc.language.iso', # linguagem que foi escrito todos pt_br
                    'dc.type', # tipo do repo ufsc TCC da graduação
                    'dc.rights', # direitos todos open acess ou não possuem
                    'dc.date.submitted', # poucas linhas com dados, outras colunas como temporalidade
                    'dc.contributor'] # instituição que contribuiu com o projeto UFSC


def run():
    data = pd.read_pickle(filepath_or_buffer='./extract/data/metadata.pickle')
    data.drop(inplace=True, columns=columns_for_drop)

    #Quantidade de paginas
    data['dc.pages'] = data['dc.format.extent'].apply(obtain_first_number_in_text)

    # Resumos
    data[['dc.description.abstract.pt', 'dc.description.abstract.en']] = data['dc.description.abstract'].apply(separate_languages)
    data['dc.description.abstract.pt'] = data['dc.description.abstract.pt'].apply(lambda abstract: abstract if type(abstract) != list else abstract[0])
    data['dc.description.abstract.en'] = data['dc.description.abstract.en'].apply(lambda abstract: abstract if type(abstract) != list else abstract[0])

    # Orientadores
    data['dc.contributor.advisor'] = data['dc.contributor.advisor'].apply(lambda x: x if type(x) == str else x[0])
    mapping = normalize_individual_names(data['dc.contributor.advisor'].tolist())
    data['dc.contributor.advisor'] = data['dc.contributor.advisor'].map(mapping)

    #Keywords
    data['dc.subject'] = data['dc.subject'].apply(transform_to_list)
    data_with_exploded_topics = data.explode('dc.subject')
    data_with_exploded_topics['dc.subject'] = data_with_exploded_topics['dc.subject'].fillna('')
    mapping_exploded = normalize_individual_names(data_with_exploded_topics['dc.subject'].tolist(), threshold=90)
    data_with_exploded_topics['dc.subject'] = data_with_exploded_topics['dc.subject'].map(mapping_exploded)
    data['dc.subject'] = data_with_exploded_topics.groupby(data_with_exploded_topics.index)['dc.subject'].agg(list)

    # Create Struct to LLM
    subset = data[[
        'dc.contributor.author',
        'dc.contributor.advisor',
        'dc.subject',
        'dc.title',
        'dc.date.available',
        'dc.description.abstract.pt',
        'dc.description.abstract.en',
        'dc.pages'
    ]]
    data['llm+string'] = subset.apply(
        lambda row: json.dumps(
            {k: v.tolist() if isinstance(v, np.ndarray) else v for k, v in row.to_dict().items()}),
        axis=1
    )

    data.to_pickle("./transform/data/metadata_transformed.pickle")
    data.to_excel("./transform/data/metadata_transformed.xlsx")


if __name__ == '__main__':
    run()

