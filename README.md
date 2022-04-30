# fhir-fsh

## Objetivo
* Desenvolver scripts em python para auxiliar na importação de recursos do site do Simplifier (https://simplifier.net/RedeNacionaldeDadosemSaude)

## Requisitos funcionais
- [x] Download automático dos recursos FHIR do projeto RNDS no site do Simplifier
- [x] Descompactação e leitura de dados dos arquivos JSON que permitem identificar os recursos (resourceType, id, url, name, title)
- [ ] Verificação de versionamento e diff: os recursos anteriormente processados foram alterados? O que alterou? Houve versionamento? Existem novos arquivos? Algum arquivo foi renomeado?
- [x] Verificar inconsistências entre os recursos: id nulo ou duplicado, nome nulo ou duplicado
- [ ] Extrair as referências dos recursos e checar a correspondência entre os arquivos locais, considerando URL, nome e id
- [ ] Recompilar referências e alterar os arquivos JSON
- [ ] Chamada de ferramenta externa para realizar o Snapshot
- [ ] Chamada de ferramenta externa para conversão dos recursos JSON em FSH


## Melhorias
* No script de checagem de referências com erro, excluir a lista de recursos e extensions canônicas
