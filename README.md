# Pipeline para Extrair e Inserir Dados do Pipedrive para SQL Server

**Cronograma Projeto ETL Pipedrive to SQL Server - Detalhamento por Semana**

**Semana 1: Planejamento e Configuração Inicial**
- Definir os objetivos do projeto em termos de extração de dados da API do Pipedrive e carregamento no SQL Server.
- Criar um repositório Git para versionamento do código.
- Configurar um ambiente virtual Python para isolar as dependências do projeto.
- Definir e criar um arquivo `.env` para armazenar variáveis sensíveis, como tokens de API e credenciais de banco de dados.
- Iniciar um arquivo README para documentar o projeto, pré-requisitos e instruções de uso.

**Semana 2: Estrutura de Extração e Inserção**
- Criar um arquivo `main.py` que servirá como ponto de entrada para o script de ETL.
- Importar as bibliotecas necessárias, como `requests`, `pyodbc` (para conexão com o SQL Server), e outras que você possa precisar.
- Implementar a função de extração, utilizando a biblioteca `requests`, para fazer as solicitações HTTP aos endpoints relevantes da API do Pipedrive e receber os dados em formato JSON.
- Implementar a função de inserção, utilizando a biblioteca `pyodbc`, para formatar os dados extraídos e inseri-los na tabela apropriada do SQL Server.

**Semana 3: Implementação das Transformações**
- Identificar quais campos da API do Pipedrive representam datas e horas. Utilize bibliotecas como `datetime` para converter os valores em objetos `datetime` apropriados.
- Implementar lógica para lidar com campos nulos durante a inserção no banco de dados. Pode-se usar condições para definir um valor padrão ou para pular a inserção da linha se um campo essencial estiver ausente.
- Formatar adequadamente valores monetários e quaisquer outros campos que exijam formatação especial, como valores percentuais ou códigos de status.

**Semana 4: Testes, Validação e Conclusão**
- Criar casos de teste para verificar a funcionalidade do script em diferentes cenários, como dados variados, campos ausentes e erros de conexão.
- Configurar um ambiente de teste separado, se necessário, para garantir que os testes não afetem o ambiente de produção.
- Executar os casos de teste e identificar possíveis problemas ou exceções não tratadas.
- Validar os dados inseridos no banco de dados SQL Server, verificando se os tipos de dados e os valores estão corretos.
- Certificar-se de que as instruções detalhadas para configurar e executar o projeto estão disponíveis na documentação.
- Revisar o README para garantir que todas as etapas e informações importantes estejam claras e bem documentadas.
- Fazer os ajustes finais no código com base nos resultados dos testes e na revisão da documentação.

## Pipeline para Extrair e Inserir Dados do Pipedrive para SQL Server

Este repositório contém um script Python para extrair dados da API do Pipedrive e inseri-los em uma tabela no SQL Server. O código foi desenvolvido para ser executado periodicamente para manter os dados sincronizados.

## Como Funciona

1. O script faz uma solicitação à API do Pipedrive para obter um conjunto de negócios (deals).
2. Os dados relevantes, como ID, título e valor dos negócios, são extraídos da resposta da API.
3. Os dados extraídos são inseridos em uma tabela no banco de dados SQL Server.
4. O processo continua para todas as páginas de resultados da API.

## Requisitos

- Python 3.x instalado
- Bibliotecas Python: requests, os, dotenv, pyodbc

## Configuração

1. Crie um arquivo `.env` na raiz do projeto e configure as seguintes variáveis:

2. Crie uma tabela no banco de dados SQL Server para armazenar os dados (nome da tabela: `deals`) com as colunas `id`, `title` e `value`.

3. Execute o script `extract_insert.py` para iniciar o processo de extração e inserção de dados.

## Sugestões de Melhorias

1. **Encapsulamento em Funções:** O código pode ser modularizado em funções para melhorar a organização.

2. **Logging de Erros:** Use um módulo de logging para registrar erros de maneira estruturada.

3. **Tratamento de Retorno da API:** Além de verificar o código de status, verifique outros campos para garantir uma solicitação bem-sucedida.

4. **Configurações Flexíveis:** Tornar o código mais flexível para personalizar endpoints e configurações.

5. **Utilização de Classes:** Considere usar classes para organizar o código de forma mais eficiente.

6. **Testes Automatizados:** Crie testes automatizados para verificar a funcionalidade do código.

## Observações

Este código é uma base inicial e pode ser aprimorado e adaptado para atender às necessidades específicas do projeto. Use as sugestões de melhorias como guia para otimizar o código.

