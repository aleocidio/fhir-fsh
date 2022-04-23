# Scrapper de arquivos do Simplifier

# Importando dependências
import requests
import zipfile
import io
import os
import json
import pandas as pd
import logging

# Variáveis globais
url_download = 'https://simplifier.net/redenacionaldedadosemsaude/download?format=json'
output_dir = 'simplifierRNDS/'
current_dir = os.getcwd()

#log
log_level = logging.DEBUG
log_fmt = '[%(levelname)s] %(asctime)s - %(message)s'


# Dataframe para armazenar resultados
recursos = pd.DataFrame()
# Quando o pandas recebe uma lista de dicionários, cada dict é uma linha,
# as chaves são as colunas e os valores são os valores das células

recursos_list = []

def download(url):
    """Baixa o arquivo do simplifier contendo os json de recursos e extrai o zip"""

    logging.info("Iniciando download")
    req = requests.get(url_download)


    with io.open('simplifierRNDS.zip','wb') as output_file:
        output_file.write(req.content)
    logging.info('Download concluído')
    
    try:
        with zipfile.ZipFile("simplifierRNDS.zip", mode="r") as archive:
            for info in archive.infolist():
                logging.info(f"Descompactando {info.filename}")
                archive.extract(info.filename, path=output_dir)
    except zipfile.BadZipFile as error:
        logging.error(error)


if __name__ == '__main__':
    logging.basicConfig(level = log_level, format=log_fmt)
    logging.info('Iniciando aplicação')   
    download(url_download)
    
    arquivos_json = os.listdir(os.path.join(current_dir, output_dir))

    for arquivo in arquivos_json:
        file_dir = os.path.join(current_dir, output_dir, arquivo)

        try:
            f = io.open(file_dir, encoding="utf-8")
            logging.info(f"Processando {arquivo}")

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
                    dict['lastUpdated'] = ''
                
            recursos_list.append(dict)
            f.close()

        except UnicodeDecodeError as error:
            f.close()
            logging.error(error)
    
    logging.info('Gerando relatório')

    recursos = pd.DataFrame(recursos_list)
    recursos.to_excel('recursos.xlsx')

    logging.info('Relatório gerado em excel')



# data quality
# recursos sem id
# recursos sem versao
# analisar o cross reference

