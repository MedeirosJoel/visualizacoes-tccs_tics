import threading
import pandas as pd
import os
import time
import requests
from queue import Queue

from pandas import DataFrame
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

file_links_path = "./extract/data/links.txt"

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_metadata_from_uri(uri: str):
    try:
        response = requests_retry_session().get(uri)
        response.raise_for_status()
        tables = pd.read_html(response.text, attrs={"class": "ds-includeSet-table"})
        if tables:
            df = tables[0].drop(2, axis=1).set_index(0).T
            return df
        else:
            print(f"Nenhuma tabela encontrada em {uri}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição para {uri}: {e}")
        return None

def many_identical_columns_for_one_column(df):
    if df is None:
        return pd.DataFrame()

    new_df = pd.DataFrame()
    columns = df.columns.tolist()
    set_columns = set(columns)

    for unique_column in set_columns:
        if columns.count(unique_column) > 1:
            new_df[unique_column] = list(df[unique_column].values)
        else:
            new_df[unique_column] = df[unique_column].values

    return new_df

def execute(link: str):
    try:
        df = get_metadata_from_uri(link)

        if df is not None:
            df = many_identical_columns_for_one_column(df)
            return df
        raise ValueError
    except Exception as e:
        print(f"Erro ao processar o link {link}: {e}")
        return None


def run_parallel(links, num_threads=8):
    ini = time.time()
    data = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_link = {executor.submit(execute, link): link for link in links}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                result = future.result()
                if result is not None:
                    data.append(result)
            except Exception as e:
                print(f"Erro ao processar o link {link}: {e}")

    print(f"Tempo total: {time.time() - ini:.2f}s")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def run_and_save():
    with open(file_links_path, "r") as file_links:
        links = file_links.read().strip().split("\n")

    data: DataFrame = run_parallel(links)

    data.to_csv("./extract/data/metadata.csv", index=False, sep=';')
    data.to_excel("./extract/data/metadata.xlsx", index=False)
    data.to_pickle("./extract/data/metadata.pickle")


if __name__ == "__main__":
    run_and_save()
