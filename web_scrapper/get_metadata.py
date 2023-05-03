from multiprocessing import Pool
import time
import pandas as pd
import get_all_links


file_links = open("./web_scrapper/data/links.txt", "r+")
links = file_links.read().split("\n")
file_links.close()


def get_metadata_from_uri(uri: str):
    df = pd.read_html(uri, attrs={"class": "ds-includeSet-table"})
    df = df[0].drop(2, axis=1).set_index(0).T
    return df


def many_indetical_columns_for_one_column(df):
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
    return many_indetical_columns_for_one_column(get_metadata_from_uri(link))


def run(links=links):
    ini = time.time()
    data = []

    for link in links[0:-1]:
        data.append(execute(link))

    print(time.time() - ini)
    return data


def run_parallel(links=links, num_processes=8):
    ini = time.time()

    with Pool(processes=num_processes) as pool:
        data = pool.map(execute, links[:-1])

    print(time.time() - ini)
    return pd.concat(data, ignore_index=True)


def save_df(type: str, df: pd.DataFrame):
    save_types = {
        "xlsx": df.to_excel("./web_scrapper/data/dataframe.xlsx", index=False),
        "csv": df.to_csv("./web_scrapper/data/dataframe.csv", index=False, sep=";"),
        "json": df.to_json("./web_scrapper/data/dataframe.json", force_ascii=False),
    }
    save_types[type]


if __name__ == "__main__":
    b = run_parallel()
    save_df("json", b)
    save_df("xlsx", b)
    save_df("csv", b)
