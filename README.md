# fhir-fsh

## Objetivo
* Desenvolver scripts em python para auxiliar na importação de recursos do site do Simplifier (https://simplifier.net/RedeNacionaldeDadosemSaude)

## Observações
* As datas serão tratadas em UTC

## Requisitos funcionais
*Baixar arquivos do Simplifier*
- [x] Download do *.zip dos recursos FHIR do projeto no site do Simplifier com conteúdo em JSON
- [x] Descompactação e leitura de dados dos arquivos JSON que permitem identificar os recursos (resourceType, id, url, name, title)

*Scrapping de informações do site Simplifier*
- [x] Scrapper para ler os metadados e id dos recursos no Simplifier passando um projeto como referência

- [x] Criar um hash sha256 de cada arquivo para checagem de alterações no arquivo

- [ ] Verificação de diff no projeto
    - [ ] novos arquivos adicionados?
    - [ ] arquivos removidos?
    - [ ] arquivos atualizados? -> alteração no SHA256

- [ ] Verificação de versionamento e diff para arquivos: 
    - [ ] Elementos adicionados?
    - [ ] Elementos removidos?
    - [ ] Valores autalizados?

    os recursos anteriormente processados foram alterados? O que alterou? Houve versionamento? Existem novos arquivos? Algum arquivo foi renomeado?

*Snapshot*
- [ ] Função para baixar o snapshot do recurso so site do Simplifier

*Checagem de consistência dos recursos* 

- [x] Verificar inconsistências entre os recursos: id nulo ou duplicado, nome nulo ou duplicado
- [X] Extrair as referências dos recursos e checar a correspondência entre os arquivos locais, considerando URL, nome e id
- [ ] Resolução de "links quebrados" dos recursos 
- [ ] Atualizar os arquivos JSON com os links corrigidos

*Banco de dados*
- [x] Armazenar cada processamento (download, scrap)
- [x] Armazenar os recursos baixados
- [ ] Armazenar os resultados dos checks de qualidade
- [ ] Armazenar os arquivos alterados com histórico de alteração


*Processamento*
- [ ] Chamada de ferramenta externa para conversão dos recursos JSON em FSH



## Melhorias
- [ ] Obter lista de recursos canônicos do site do FHIR, de acordo com a versão (inicialmente apenas R4)
