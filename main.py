# Scrapper de arquivos do Simplifier

# o código abaixo inicia o ipython no vscode

#%%

# Importando dependências
from typing import List
from pyparsing import infix_notation
import requests
import zipfile
import io
import os
import json
import pandas as pd
import logging as l
from jsonpath_rw import parse, jsonpath

def download(url):
    """Baixa o arquivo do simplifier contendo os json de recursos e extrai o zip"""

    l.info("Iniciando download")
    req = requests.get(url_download)

    # se houver arquivo previamente baixado, irá sobrescrever
    with io.open('simplifierRNDS.zip','wb') as output_file:
        output_file.write(req.content)
    print('Download concluído')
    
    try:
        with zipfile.ZipFile("simplifierRNDS.zip", mode="r") as archive:
            for info in archive.infolist():
                l.info(f"Descompactando {info.filename}")
                archive.extract(info.filename, path=output_dir)

    except zipfile.BadZipFile as error:
        l.error(error)

def eh_url(str):
    if "://" in str:
        return True
    return False
def eh_canonica(str):
    for i in url_canonica:
        if i in str:
            return True
    return False

def lista_referencias(arquivo, current_dir, output_dir):
    file_dir = os.path.join(current_dir, output_dir, arquivo)
    
    try:
            # abre o arquivo json
            f = io.open(file_dir, encoding="utf-8")
            l.info(f"Processando {arquivo}")
            # carrega em uma classe json
            data = json.load(f)
            # lista temporária para receber todas as referências feitas
            temp_list = []
            # busca dentro do json os jsonpath definidos no início do programa
            for key, value in json_path_expr_dict.items():
                json_path_expr = parse(value)
                busca = json_path_expr.find(data)
                for i in busca:
                    temp_list.append(i.value)            
            # fecha o arquivo
            f.close()
     # tratamento de exceções de Decode
    except UnicodeDecodeError as error:
        f.close()
        l.error(error)
    return temp_list

def quality_report(df):
    l.info('Iniciando checagem de data quality')
    print()
    print('+--------------- DATA QUALITY ---------------+')
    print(f"Total de recursos: {recursos.shape[0]}")
    total_sem_id = df.replace({'':pd.NA})['id'].isna().sum()
    total_id_duplicado = df.loc[df['id'] != ''].duplicated('id').sum()
    total_sem_versao = df.replace({'':pd.NA})['version'].isna().sum()
    print(f"Recuros sem id:  {total_sem_id}")
    print(f"Recursos sem versao: {total_sem_versao}")
    print(f"Ids duplicados: {total_id_duplicado}")
    return

def reference_check(df):
    print("+--------------- REFERENCIAS ----------------+")
    # checagem de referencias
    # cria listas auxiliares para etapas de verificacao
    url_index = df['url'].to_list()
    id_index = df['id'].to_list()
    name_index = df['name'].to_list()

    erros = []

    for index, row in df.iterrows():
        # checa se existe alguma referência
        if len(row['referencias']) > 0:
            for ref in row['referencias']:
                if eh_url(ref):
                    if eh_canonica(ref) == False:
                        if ref not in url_index:
                            erros.append({"index":index,"type":"URL NOT FOUND","reference_error": ("URL NOT FOUND " + ref) })
                #se não for URL
                else:
                    if (ref not in id_index) or (ref not in name_index):
                            erros.append({"index":index,"type":"NAME ID NOT FOUND","reference_error": ("NAME/ID NOT FOUND " + ref)})
    return pd.DataFrame(erros).groupby('index').agg(pd.Series.tolist)

#%%

if __name__ == '__main__':
    l.basicConfig(level = l.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
    l.info('Iniciando aplicação')
    
    # Variáveis globais

    url_download = 'https://simplifier.net/redenacionaldedadosemsaude/download?format=json'
    # URL de saída dos json de recursos após download e descompactação
    output_dir = 'simplifierRNDS/'
    # diretório onde o programa está rodando
    current_dir = os.getcwd()
    # URLS consideradas canônicas quando contêm
    url_canonica = ["http://terminology.hl7.org","http://hl7.org/fhir/"]
    # Dataframe para armazenar os recursos com metadados
    recursos = pd.DataFrame()
    # lista temporária auxiliar para montar o dataframe
    recursos_list = []

    # json path para buscar as referências dentro dos recursos
    # https://jsonpath.com/

    json_path_expr_dict = {
        "Lista dos profiles": "$.*.*[:].*[:].profile[:]",
        "Lista de valueSets": "$.*.*[:].binding.valueSet",
        "Lista de Uri extensions": "$.*.*[:].fixedUri",
        "Includes de valueSets": "$.*.*[:].system",
        "ValueSet usado em Composition": "$.*.*.*.*.valueSet",
        "Profiles usados em Composition": "$.*.*.*.*.*.profile[:]",
        "Links em Composition": "$.*.*.*.*.*.targetProfile[:]"
    }

    download(url_download)
    
    # lista os arquivos json no diretório output_dir
    arquivos_json = os.listdir(os.path.join(current_dir, output_dir))

    # percorre todos os arquivos json para extração de informações
    for arquivo in arquivos_json:
        file_dir = os.path.join(current_dir, output_dir, arquivo)

        try:
            # abre o arquivo json
            f = io.open(file_dir, encoding="utf-8")
            l.info(f"Processando {arquivo}")
            # carrega em uma classe json
            data = json.load(f)
            # inicia um dicionário em branco
            dict = {}
            # extrai no nome do arquivo
            dict['filename'] = arquivo
            # extração das principais chaves para o dicionário
            keys = ['resourceType', 'id', 'name', 'title',
                    'url', 'version', 'status', 'publisher']
            for key in keys:
                try:
                    dict[key] = data[key]
                except KeyError:
                    dict[key] = ''
                # extrai os metadados
                try:
                    dict['lastUpdated'] = data['meta']['lastUpdated']
                except KeyError:
                    dict['lastUpdated'] = ''
            # adiciona o dicionário a lista que irá alimentar o DataFrame
            recursos_list.append(dict)

            # busca referências dentro do recurso
            dict['referencias'] = lista_referencias(arquivo,current_dir,output_dir)
            # fecha o arquivo
            f.close()
        # tratamento de exceções de Decode
        except UnicodeDecodeError as error:
            f.close()
            l.error(error)
    # salva os recursos em um DataFrame
    recursos = pd.DataFrame(recursos_list)
    quality_report(recursos)
    referencias_erro = reference_check(recursos)
    final_report = pd.merge(recursos, referencias_erro[['reference_error']], how='left', left_index=True, right_index=True)

    #exporta para Excel
    try:
        final_report.to_excel('recursos.xlsx')
    except PermissionError:
        l.error("Erro ao salvar recursos.xlsx, tentando com outro nome de arquivo.")
        final_report.to_excel('recursos(1).xlsx')
    l.info('Gerado relatório')

# %%

referencias_erro.index
# %%
final_report.iloc[[13, 14, 16, 17, 24, 25, 28, 32, 40, 52, 53, 59, 82, 83, 110]]
# %%
final_report.loc[13]['reference_error']


# http://www.saude.gov.br/fhir/r4/ValueSet/BRCondicaoMaternal-1.0
# http://www.saude.gov.br/fhir/r4/StructureDefinition/BRCondicaoMaternal

# %%
recursos.loc[recursos['name'].str.contains('matern', case=False)]['url']