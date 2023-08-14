# Pipeline para Extrair e Inserir Dados do Pipedrive para SQL Server

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

6. **Gestão de Tokens:** Mantenha informações sensíveis fora do código fonte e use variáveis de ambiente.

7. **Documentação e Comentários:** Adicione comentários explicativos ao código.

8. **Testes Automatizados:** Crie testes automatizados para verificar a funcionalidade do código.

## Observações

Este código é uma base inicial e pode ser aprimorado e adaptado para atender às necessidades específicas do projeto. Use as sugestões de melhorias como guia para otimizar o código.

