import os
import datetime
from dotenv import load_dotenv
import pandas as pd
from extract import extract_data
from transform import transform_produtos, transform_producao, transform_faturamento, transform_f9, transform_carteira
from load import save_to_excel

def main():
    """Executa o pipeline ETL para processar dados de dashboard gerencial."""
    try:
        # Carrega variáveis de ambiente
        load_dotenv()
        base_path = os.getenv("BASE_PATH", "data/")
        output_file = os.getenv("OUTPUT_FILE", "output.xlsx")

        # Lista de empresas e arquivos
        empresas = {
            "Empresa_A": {
                "produtos": "sample_empresa_a_file1.csv",
                "producao": "sample_empresa_a_file2.csv",
                "faturamento": "sample_empresa_a_file3.csv",
                "carteira": "sample_empresa_a_file4.csv",
                "f9": "sample_empresa_a_file5.csv",
            },
            "Empresa_B": {
                "produtos": "sample_empresa_b_file1.csv",
                "producao": "sample_empresa_b_file2.csv",
                "faturamento": "sample_empresa_b_file3.csv",
                "carteira": "sample_empresa_b_file4.csv",
                "f9": "sample_empresa_b_file5.csv",
            },
        }

        # Processa dados para cada empresa
        produtos_dfs = []
        producao_dfs = []
        faturamento_dfs = []
        carteira_dfs = []
        f9_dfs = []

        for empresa, files in empresas.items():
            # Extração
            df_produtos = extract_data(os.path.join(base_path, files["produtos"]), empresa)
            df_producao = extract_data(os.path.join(base_path, files["producao"]), empresa)
            df_faturamento = extract_data(os.path.join(base_path, files["faturamento"]), empresa)
            df_carteira = extract_data(os.path.join(base_path, files["carteira"]), empresa)
            df_f9 = extract_data(os.path.join(base_path, files["f9"]), empresa)

            # Transformação
            df_produtos, merge_produtos = transform_produtos(df_produtos, empresa)
            df_producao = transform_producao(df_producao, merge_produtos, empresa)
            df_faturamento = transform_faturamento(df_faturamento, empresa)
            df_carteira = transform_carteira(df_carteira, merge_produtos, empresa)
            df_f9 = transform_f9(df_f9, empresa)

            # Armazena DataFrames
            produtos_dfs.append(df_produtos)
            producao_dfs.append(df_producao)
            faturamento_dfs.append(df_faturamento)
            carteira_dfs.append(df_carteira)
            f9_dfs.append(df_f9)

        # Concatena DataFrames
        produtos = pd.concat(produtos_dfs, ignore_index=True)
        producao = pd.concat(producao_dfs, ignore_index=True)
        faturamento = pd.concat(faturamento_dfs, ignore_index=True)
        carteira = pd.concat(carteira_dfs, ignore_index=True)
        f9 = pd.concat(f9_dfs, ignore_index=True)

        # Transformações adicionais
        produtos.loc[produtos["Grupo de producao"] == "BOBINA", "Grupo de producao"] = "BOBINA VENDA"
        carteira = transform_carteira_additional(carteira, produtos)

        # Gera DataFrame de atualização
        update = pd.DataFrame({"Atualizacao": [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]})

        # Salva no Excel
        save_to_excel(
            {
                "Produtos": produtos,
                "Producao": producao,
                "Faturamento": faturamento,
                "Carteira": carteira,
                "F9": f9,
                "Atualizacao": update,
            },
            os.path.join(base_path, output_file),
        )

        print(f"Arquivo Excel salvo em: {os.path.join(base_path, output_file)}")

    except Exception as e:
        print(f"Erro no pipeline ETL: {str(e)}")

if __name__ == "__main__":
    main()