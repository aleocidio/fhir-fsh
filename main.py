# Scrapper de arquivos do Simplifier

# Importando dependências
import requests
import zipfile
import io
import os
import json
import pandas as pd

# Variáveis globais

url_download = 'https://simplifier.net/redenacionaldedadosemsaude/download?format=json'
output_dir = 'simplifierRNDS/'
current_dir = os.getcwd()


# Dataframe para armazenar resultados
recursos = pd.DataFrame()
# Quando o pandas recebe uma lista de dicionários, cada dict é uma linha,
# as chaves são as colunas e os valores são os valores das células

recursos_list = []

def download(url):
    """Baixa o arquivo do simplifier contendo os json de recursos e extrai o zip"""

    print("Iniciando download")
    req = requests.get(url_download)


    with io.open('simplifierRNDS.zip','wb') as output_file:
        output_file.write(req.content)
    print('Download concluído')
    
    try:
        with zipfile.ZipFile("simplifierRNDS.zip", mode="r") as archive:
            for info in archive.infolist():
                print("Descompactando", info.filename)
                archive.extract(info.filename, path=output_dir)
                print("Descompactado")
    except zipfile.BadZipFile as error:
        print(error)


if __name__ == '__main__':
    print('#### START #####')
    
    download(url_download)
    
    arquivos_json = os.listdir(os.path.join(current_dir, output_dir))

    for arquivo in arquivos_json:
        file_dir = os.path.join(current_dir, output_dir, arquivo)

        try:
            f = io.open(file_dir, encoding="utf-8")
            print("Processando ", arquivo)

            data = json.load(f)
            
            keys = ['url','status','version','publisher','id','resourceType','name']
            dict = {}
            for key in keys:
                try:
                    dict[key] = data[key]
                except KeyError:
                    dict[key] = ''

                try:
                    dict['lastUpdated'] = data['meta']['lastUpdated']
                except KeyError:
                    dict['meta'] = ''
                
            recursos_list.append(dict)
            f.close()

        except UnicodeDecodeError as error:
            f.close()
            print(error)
    
    print('Gerando relatório')

    recursos = pd.DataFrame(recursos_list)
    recursos.to_excel('recursos.xlsx')

# data quality
# recursos sem id
# recursos sem versao
# analisar o cross reference

