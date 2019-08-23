# ibmcloud-psql

Este repositório contém pedaços de código para facilitar o uso do PostgreSQL na IBM Cloud. Ele automatiza a leitura e extração do JSON gerado pela IBM Cloud com as credenciais do serviço, prontas para serem usadas com a biblioteca Python `psycopg2`.

## Como utilizar

- Crie uma instância do [PostgreSQL na IBM Cloud](https://cloud.ibm.com/catalog/services/databases-for-postgresql)
- Na página Web do serviço instanciado na IBM Cloud, clique na aba `service credentials`.
- Crie uma nova credencial se necessário, e copie todo o conteúdo do JSON gerado.
- Cole o conteúdo do JSON no arquivo `ipsql_credentials.json`.
- Já é possível executar o código do arquivo `ipsql.py`!
