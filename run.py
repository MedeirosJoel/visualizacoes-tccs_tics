from extract.get_all_links import run as run_links_extraction
from extract.get_metadata import run_and_save as run_data_extraction
from transform.transform import run as run_transform

if __name__ == '__main__':
    run_links_extraction()
    run_data_extraction()
    run_transform()