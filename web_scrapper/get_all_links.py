from multiprocessing import Pool
import time
import requests
from bs4 import BeautifulSoup

initial_url = (
    "https://repositorio.ufsc.br/handle/123456789/41812/recent-submissions?offset={}"
)

full_papper = 'https://repositorio.ufsc.br/{}?show=full'

amount_pappers = int(
    BeautifulSoup(requests.get(initial_url).text, "html.parser")
    .find("p", {"class": "pagination-info"})
    .text.split(" ")[-1]
)

ammount_index_pages = (int(amount_pappers / 20) 
                       + 1 if (amount_pappers % 20 != 0) else 0)

index_offsets = [i * 20 for i in range(ammount_index_pages)]


def get_links(index, trys = 0) -> list:
    """
    Dado um offset de indice de uma pagina de trabalhos de TCC retorna o href de cada um dentro da lista
    """
    try:
        html = requests.get(initial_url.format(index)).text
    except:
        if trys < 10:
            return get_links(index, trys+1)
        
        raise ConnectionRefusedError(None)
    
    soup = (
        BeautifulSoup(html, "html.parser")
        .find("ul", {"class": "ds-artifact-list"})
        .find_all("a")
    )

    return [full_papper.format(link.get("href")) for link in soup]

def get_all_links():
    """Utilizando o offset das paginas de indices, adquire todos os links de trabalhos dessas paginas """
    with Pool() as p:
        final_list = p.map(get_links, index_offsets)
    
    return set([element for sublist in final_list for element in sublist])
    
         

def write_on_file(links):
    with open('./web_scrapper/data/links.txt', 'w+') as file:
        file.writelines([link + '\n' for link in links])

if __name__ == '__main__':
    ini = time.time()
    links = get_all_links()
    write_on_file(links)
    print(time.time()- ini)
    