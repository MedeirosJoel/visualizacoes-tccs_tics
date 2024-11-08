import pandas as pd # dataframe manipulation
import numpy as np # linear algebra
from sentence_transformers import SentenceTransformer




def compile_text(x):

    text = f"""
    URI: {x['dc.identifier.uri']}; 
    Date Issued: {x['dc.date.issued']}; 
    Language: {x['dc.language.iso']}; 
    Rights: {x['dc.rights']}; 
    Advisor: {x['dc.contributor.advisor']}; 
    Subject: {x['dc.subject']}; 
    Type: {x['dc.type']}; 
    Author: {x['dc.contributor.author']}; 
    Description: {x['dc.description']}; 
    Abstract: {x['dc.description.abstract']}; 
    Date Available: {x['dc.date.available']}; 
    Title: {x['dc.title']}; 
    Contributor: {x['dc.contributor']};  
    Co-Advisor: {x['dc.contributor.advisor-co']}; 
    Title Alternative: {x['dc.title.alternative']}; 
    Date Submitted: {x['dc.date.submitted']}; 
    Subject Classification: {x['dc.subject.classification']}"""

    return text

def run_embedding(save_in='./llm +kmeans/data/embedding.csv', read_at='./extract/data/dataframe.csv'):
    df = pd.read_csv(read_at, sep=";")
    sentences = df.apply(lambda x: compile_text(x), axis=1).tolist()
    model = SentenceTransformer(r"sentence-transformers/paraphrase-MiniLM-L6-v2")
    output = model.encode(sentences=sentences,
         show_progress_bar=True,
         normalize_embeddings=True)
    df_embedding = pd.DataFrame(output)
    df_embedding.to_csv(save_in, index=False)

run_embedding()
