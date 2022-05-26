#%%
from bs4 import BeautifulSoup
import requests
import pandas as pd
import io
from lxml import etree
import re


def calcula_paginas(registros_total, registros_pagina) -> int:
    i = registros_total//registros_pagina
    paginas_calc = 1
    while i != 0:
        paginas_calc += 1
        registros_total = registros_total - registros_pagina
        i = registros_total//registros_pagina    
    return paginas_calc

def download_info():    
    # define variáveis acessórias
    registros_total = 0
    registros_pagina = 100
    pagina = 1
    paginas_total = 0
    
    retorno = []
    # l.info("Iniciando POST Request para simplifier.net")
        
    # define o payload para chamada na API -> retorna o total de registros
    payload = {
        "PaginationViewModel.SelectedPage": pagina,
        "PaginationViewModel.SelectedPageSize": registros_pagina,
        "ProjectKey": "RedeNacionaldeDadosemSaude",
        "SelectedFhirVersions": "R4",
        "Sort.SelectedProperty": "RankScore_desc"
    }
    call = requests.post('https://simplifier.net/redenacionaldedadosemsaude/filterprojectpublications', data=payload)
    soup = BeautifulSoup(call.content, 'html.parser')
    registros_total = int(soup.find("input", {"name":"PaginationViewModel.TotalCount"})['value'])
    
    # l.info(f"Encontrados {registros_total} registros para o projeto")
    
    # calcula o total de paginas para realizar o scrapping
    paginas_total = calcula_paginas(registros_total,registros_pagina)
        
    # loop para cada página de resultado
    for i in range(0,paginas_total):
        print(f"Processando página {i+1} de {paginas_total}")
        payload = {
            "PaginationViewModel.SelectedPage": i,
            "PaginationViewModel.SelectedPageSize": registros_pagina,
            "ProjectKey": "RedeNacionaldeDadosemSaude",
            "SelectedFhirVersions": "R4",
            "Sort.SelectedProperty": "RankScore_desc"
        }
        call = requests.post('https://simplifier.net/redenacionaldedadosemsaude/filterprojectpublications', data=payload)
        soup = BeautifulSoup(call.content, 'html.parser')
        retorno.append(soup)

    return retorno   

#%%
def busca_url_canonica(link: str) -> str:
    call = requests.get(link)
    soup = BeautifulSoup(call.content, 'html.parser')
    canonica = soup.find('input', attrs={'id':'canonical-url-to-copy'}).get('value')
    return canonica

def busca_nome_arquivo(link: str) -> str:
    """Busca o nome do arquivo de download no simplifier"""
    #stream = True -> baixa apenas os headers
    req = requests.get(link + '/$download?format=json', stream=True)
    header = req.headers['Content-Disposition']
    filename = re.search("(?<=filename=)(.*)(?=;)", header).group()
    filename = filename.replace('"', '')
    return filename

def scrap_info(infos: list) -> list:
    
    print("Inciando scrapping de informações")
    
    dict_tmp = {}
    retorno = []
    pgs = len(infos)    
    
    for info in infos:
        div_linha = info.select('div.row')
        #somente para debug
        pg = 1
        for i in range(0,len(div_linha)-1):
            print(f"Pág {pg} / {pgs}. Arquivo {i+1} de {len(div_linha)-1}")
            LINK=div_linha[i].find('a').get('href')
            NAME=div_linha[i].find('a').text
            type_div=div_linha[i].find('div',attrs={'class':'type'})
            TYPE = type_div.find('b').text
            tag_div = div_linha[i].find('span',attrs={'class':'tag'})
            TAG = tag_div.find('a').text
            datetime_div = div_linha[i].find('time')
            DATETIME = datetime_div.get('datetime')
            CANONICA = busca_url_canonica(LINK)
            ARQUIVO = busca_nome_arquivo(LINK)
            dict_tmp = {
                "NAME":NAME,
                "LINK":LINK,
                "CANONICA":CANONICA,
                "ARQUIVO": ARQUIVO,
                "TYPE":TYPE,
                "TAG":TAG,
                "DATETIME":DATETIME
            }
            retorno.append(dict_tmp)
            
            #somente para debug
        pg +=1

    return retorno

def download_snapshot(link: str):
    cmd = '/$downloadsnapshot?format=json'
    req = requests.get(link+cmd)
    
    if req.headers['Content-Type'].find('json') != -1:
        with io.open('teste.json','wb') as output_file:
            output_file.write(req.content)
    else:
        print('não existe snapshot')

def busca_url_canonica_xpath(link: str) -> str:
    call = requests.get(link)
    soup = BeautifulSoup(call.content, 'html.parser')
    dom = etree.HTML(str(soup))
    canonica = dom.xpath('//*[@id="canonical-url-to-copy"]/@value')
    return canonica


#URLS para teste
#download_snapshot('https://simplifier.net/redenacionaldedadosemsaude/unidade%20de%20tempo')
#download_snapshot('https://simplifier.net/redenacionaldedadosemsaude/brdocumentoindividuo')

#%%
#df.to_json('local.json', orient='records', index=True)

download = download_info()
scrap = scrap_info(download)
df = pd.DataFrame(scrap)

df.to_excel('site_simplifier.xlsx')
# %%
