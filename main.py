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
    total_url_duplicada = df.loc[df['url'] != ''].duplicated('url').sum()
    print(f"Recuros sem id:  {total_sem_id}")
    print(f"Recursos sem versao: {total_sem_versao}")
    print(f"Ids duplicados: {total_id_duplicado}")
    print(f"URLs duplicadas: {total_url_duplicada}")
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

#%% 
def cria_features(df):
    ''' Função para criar features que permitam comparar os recursos
    o resultado é um df em que o prefixo das colunas sinaliza a origem da informação
    fs_ filesystem
    meta_ metadados do json
    url_ da url declarada no json
    '''    
    novos_nomes_colunasA = {df.columns.values[0]:('fs_' + df.columns.values[0])}
    novos_nomes_colunasB = {c:'meta_' + c for c in df.columns.values[1:]}
    novos_nomes_colunas = {**novos_nomes_colunasA, **novos_nomes_colunasB}
    df.rename(columns = novos_nomes_colunas, inplace = True)
    df['url_resourceType'] = df['meta_url'].apply(lambda x: x.split('/')[-2])
    df['url_name'] = df['meta_url'].apply(lambda x: x.split('/')[-1])
    return df

#%%
from bs4 import BeautifulSoup

def bs_scrapper():
    payload = {
        "PaginationViewModel.SelectedPage": 1,
        "PaginationViewModel.SelectedPageSize": 100,
        "ProjectKey": "RedeNacionaldeDadosemSaude",
        "SelectedFhirVersions": "R4",
        "Sort.SelectedProperty": "RankScore_desc"
    }
    call = requests.post('https://simplifier.net/redenacionaldedadosemsaude/filterprojectpublications', data=payload)
    soup = BeautifulSoup(call.content, 'html.parser')
    total_registros = int(soup.find("input", {"name":"PaginationViewModel.TotalCount"})['value'])
    print(total_registros)
# %%
bs_scrapper()

# Objetivo:
# incremetar dataset com informações da web
# Caption
# Type
# description
# web_url
# datetime
# history
# project FHIR API
# para Structure Definition, é possivel baixar o snapshot
#snapshot: https://simplifier.net/redenacionaldedadosemsaude/brcondicaomaternal/$downloadsnapshot?format=json

# <div class="searchresult">
#            <div class="row">
#                        <a class="caption" href="https://simplifier.net/redenacionaldedadosemsaude/unidade%20de%20medida%20de%20medicamento">Unidade de Medida de Medicamento</a>
# <div class="type">
#                        <b>ValueSet</b>
#                    </div>
#                    <div class="description">
#                        ValueSet utilizado para definir a unidade de medida de medicamentos sob informa&#xE7;&#xF5;es do fabricante.
#                    </div>

#<time class="localize onlyDate" datetime="2022-04-13T19:46:20.7448577Z">13/04/2022</time>