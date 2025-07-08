import pandas as pd

def extract_data(file_path, empresa):
    """
    Extrai dados de um arquivo CSV.

    Args:
        file_path (str): Caminho do arquivo CSV.
        empresa (str): Nome da empresa.

    Returns:
        pd.DataFrame: DataFrame com os dados extraídos.
    """
    try:
        df = pd.read_csv(file_path, encoding="latin1", sep=";")
        df["Empresa"] = empresa
        return df
    except FileNotFoundError:
        raise Exception(f"Arquivo {file_path} não encontrado.")
    except Exception as e:
        raise Exception(f"Erro na extração de {file_path}: {str(e)}")