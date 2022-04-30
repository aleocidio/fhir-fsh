# Scrapper de arquivos do Simplifier

# o código abaixo inicia o ipython no vscode

#%%


# Importando dependências
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

def quality_report(df: pd.DataFrame()) -> pd.DataFrame():
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
    print("+--------------- REFERENCIAS ----------------+")
    # checagem de referencias
    # cria listas auxiliares para etapas de verificacao
    referencias_index = df['url'].to_list()
    id_index = df['id'].to_list()
    name_index = df['name'].to_list()
    # lista para armazenar os erros encontrados
    lista_erros_ref = []
    # iteração no dataframe, linha a linha
    for index, row in df.iterrows():
        msg_stream = []
        ref_array = []
        # resgata o array de referências do recurso
        ref_array = row['referencias']
        # para cada referência do recurso
        for r in ref_array:
            # checa se é URL
            if eh_url(r):
                # se for URL, verifica se algum recurso dos json tem a mesma url
                if r not in referencias_index:
                    # se não encontrado, checa se é URL canônica
                    if eh_canonica(r) == False:
                        msg = f"A referencia da URL {r} do recurso {index} é inválida"
                        print(msg)
                        msg_stream.append(msg)
            else:
                # se não for URL, verifica por id ou nome
                if (r not in id_index) or (r not in name_index):
                    msg = f"A referencia {r} do recurso {index} não tem correspondente de nome ou id"
                    print(msg)
                    msg_stream.append(msg)
        #agrupa os erros encontrados vinculando o index do recurso no dataframe com a lista de erros          
        lista_erros_ref.append([index, msg_stream]) 
        df_retorno = pd.DataFrame(lista_erros_ref, columns=['index', 'erros'])
        df_retorno.set_index('index', drop=True, inplace=True)
        return df_retorno

if __name__ == '__main__':
    l.basicConfig(level = l.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
    l.info('Iniciando aplicação')
    
    # Variáveis globais

    url_download = 'https://simplifier.net/redenacionaldedadosemsaude/download?format=json'
    output_dir = 'simplifierRNDS/'
    current_dir = os.getcwd()
    url_canonica = ["http://terminology.hl7.org","http://hl7.org/fhir/"]
    # Dataframe para armazenar resultados
    recursos = pd.DataFrame()

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
            # inicia uma lista para receber as referencias
            referencias = []
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
            for key, value in json_path_expr_dict.items():
                json_path_expr = parse(value)
                busca = json_path_expr.find(data)
                for i in busca:
                    referencias.append(i.value)
            dict['referencias'] = referencias
            # fecha o arquivo
            f.close()
        # tratamento de exceções de Decode
        except UnicodeDecodeError as error:
            f.close()
            l.error(error)
    # salva os recursos em um DataFrame
    recursos = pd.DataFrame(recursos_list)
    quality = quality_report(recursos)
    final_report = pd.merge(recursos, quality, how='left', left_index=True, right_index=True)

    # exporta para Excel
    try:
        final_report.to_excel('recursos.xlsx')
    except PermissionError:
        l.error("Erro ao salvar recursos.xlsx, tentando com outro nome de arquivo.")
        final_report.to_excel('recursos(1).xlsx')
    l.info('Gerado relatório')



# reconstruir ids e vínculos

# Nome é em CamelCase
# id é com - e tudo minusculo
