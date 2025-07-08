# ETL para Dashboard Gerencial

## Descrição
Este projeto implementa um pipeline ETL em Python para processar dados de estoque, produção, faturamento e pedidos de duas empresas reais representadas por (`Empresa_A` e `Empresa_B`). O pipeline consolida informações de múltiplos arquivos CSV, realiza transformações complexas (como ajustes de espessura, categorização de materiais e cálculos de métricas) e gera um arquivo Excel para alimentar dashboards gerenciais.

**Os Dataframes de exemplo e as informações sensiveis nos códigos foram simuladas/anonilizadas utilizando AI.

## Objetivo
- **Extrair**: Lê dados de arquivos CSV com informações de estoque, produção, faturamento, carteira de pedidos e aprovações.
- **Transformar**: Aplica tratamentos como conversão de datas, ajustes numéricos, categorização de materiais e merges entre datasets.
- **Carregar**: Salva os dados processados em um arquivo Excel (para facil manuseio de partes interessadas) com múltiplas abas para uso em dashboards (uso principal).


## Tecnologias Utilizadas
- Python 3.12.6
- Pandas (manipulação de dados)
- python-dotenv (gerenciamento de variáveis de ambiente)
- Openpyxl (escrita de arquivos Excel)

## Estrutura do Projeto
```
etl_dashboard/
├── src/
│   ├── extract.py       # Funções de extração de dados
│   ├── transform.py     # Funções de transformação de dados
│   ├── load.py          # Funções de carga de dados
│   ├── main.py          # Script principal do pipeline
├── data/
│   ├── sample_empresa_a_file1.csv  # Exemplo de dados de estoque (Empresa_A)
│   ├── sample_empresa_a_file2.csv  # Exemplo de dados de produção (Empresa_A)
│   ├── ...
├── .env.example         # Exemplo de configurações
├── requirements.txt     # Dependências
├── .gitignore           # Arquivos ignorados
└── README.md            # Documentação
```

## Exemplo de Saída
O script gera um arquivo `output.xlsx` com as seguintes abas:
- **Produtos**: Estoque de produtos por empresa.
- **Producao**: Dados de produção com informações de máquinas e lotes.
- **Faturamento**: Informações de vendas, incluindo valores e peso.
- **Carteira**: Pedidos em aberto com detalhes de materiais.
- **F9**: Status de aprovações de pedidos.
- **Atualizacao**: Data e hora da última execução.

## Dados de Exemplo
Os arquivos CSV na pasta `data/` contêm dados fictícios para demonstração. Eles simulam a estrutura real dos dados, mas sem informações sensíveis.

## Notas
- Configurações sensíveis (como caminhos de arquivos) devem ser definidas no arquivo `.env`.
- O código inclui tratamento de erros robusto e segue as boas práticas de modularização.
- As transformações refletem regras de negócio complexas, como ajustes de espessura e categorização de materiais.