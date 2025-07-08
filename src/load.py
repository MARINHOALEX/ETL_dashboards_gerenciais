import pandas as pd

def save_to_excel(dfs, output_path):
    """
    Salva múltiplos DataFrames em um arquivo Excel.

    Args:
        dfs (dict): Dicionário com nome da aba como chave e DataFrame como valor.
        output_path (str): Caminho do arquivo Excel de saída.
    """
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    except Exception as e:
        raise Exception(f"Erro ao salvar arquivo Excel: {str(e)}")