import threading
import pandas as pd
import os
import time
import requests
from queue import Queue
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


file_links_path = "./extract/data/links.txt"

os.makedirs(os.path.dirname(file_links_path), exist_ok=True)

with open(file_links_path, "r+") as file_links:
    links = file_links.read().split("\n")


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 503, 504),
    session=None,
):
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
        df = pd.read_html(response.text, attrs={"class": "ds-includeSet-table"})
        df = df[0].drop(2, axis=1).set_index(0).T
        return df
    except requests.exceptions.RequestException as e:
        print(f'Erro ao fazer requisição para {uri}: {e}')
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
        result = many_identical_columns_for_one_column(get_metadata_from_uri(link))
        return result
    except Exception as e:
        print(f'Erro ao processar o link {link}: {e}')
        return None


def run_parallel(links=links, num_threads=8):
    ini = time.time()
    data = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_link = {executor.submit(execute, link): link for link in links[:-1]}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                result = future.result()
                if result is not None:
                    data.append(result)
            except Exception as e:
                print(f'Erro ao processar o link {link}: {e}')

    print(time.time() - ini)
    return pd.concat(data, ignore_index=True)


def save_df(file_type: str, df: pd.DataFrame):
    save_types = {
        "xlsx": df.to_excel,
        "csv": df.to_csv,
        "json": df.to_json
    }
    save_path = f"/extract/data/dataframe.{file_type}"
    save_function = save_types[file_type]

    if file_type == "csv":
        save_function(save_path, index=False, sep=";")
    else:
        save_function(save_path, index=False)

    print(f"Arquivo salvo em: {save_path}")


if __name__ == "__main__":
    b = run_parallel()
    save_df("json", b)
    save_df("xlsx", b)
    save_df("csv", b)
