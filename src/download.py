import requests
import zipfile
from datetime import datetime
import os
import io


def download_file(url_download, output_dir):
    """
    url = url do projeto no simplifier. Ex: https://simplifier.net/redenacionaldedadosemsaude
    Baixa o arquivo zip do simplifier contendo todos os arquivos em formato json
    e descompacta o zip em uma pasta local. Apaga o .zip após descompactação.
    """

    l.info("Iniciando download")
    req = requests.get(url_download)

    nome_arquivo = datetime.now().strftime("YYYYMMDDHHMMSS")
    with io.open(nome_arquivo,'wb') as output_file:
        output_file.write(req.content)
    print('Download concluído')
    
    try:
        with zipfile.ZipFile("simplifierRNDS.zip", mode="r") as archive:
            for info in archive.infolist():
                l.info(f"Descompactando {info.filename}")
                archive.extract(info.filename, path=output_dir)

    except zipfile.BadZipFile as error:
        l.error(error)
    
    os.remove(nome_arquivo)