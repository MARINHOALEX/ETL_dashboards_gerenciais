import pandas as pd
import re

def transform_date(dataframe, column):
    """
    Converte uma coluna de data para o formato datetime.

    Args:
        dataframe (pd.DataFrame): DataFrame a ser processado.
        column (str): Nome da coluna de data.
    """
    try:
        dataframe[column] = pd.to_datetime(dataframe[column], format="%d/%m/%Y", errors="coerce")
    except Exception as e:
        raise Exception(f"Erro na conversão de data na coluna {column}: {str(e)}")

def transform_number(dataframe, column, dtype, divisor):
    """
    Converte uma coluna numérica, removendo pontos e aplicando divisor.

    Args:
        dataframe (pd.DataFrame): DataFrame a ser processado.
        column (str): Nome da coluna.
        dtype: Tipo de dado (ex.: int, float).
        divisor (float): Divisor para ajustar escala.
    """
    try:
        dataframe[column] = dataframe[column].str.replace(".", "").astype(dtype) / divisor
    except Exception as e:
        raise Exception(f"Erro na conversão numérica na coluna {column}: {str(e)}")

def transform_produtos(df, empresa):
    """
    Transforma o DataFrame de produtos.

    Args:
        df (pd.DataFrame): DataFrame de produtos.
        empresa (str): Nome da empresa.

    Returns:
        tuple: (DataFrame transformado, DataFrame para merge).
    """
    try:
        df = df[["Codigo", "Descricao", "Und.Estoque", "Espessura", "Largura", "Comprimento",
                 "Descricao.1", "Descricao.2", "Grupo de producao", "Quantidade atual"]]
        
        # Anonimiza valores de Descricao.1 e Descricao.2
        df["Descricao.1"] = df["Descricao.1"].replace({
            "CHAPA": "CATEGORIA_X",
            "BOBINA": "CATEGORIA_Y",
            # Adicione outros mapeamentos conforme necessário
        })
        df["Descricao.2"] = df["Descricao.2"].replace({
            "GALVALUME": "MATERIAL_X",
            "ZINCADA": "MATERIAL_Y",
            "FINA QUENTE": "MATERIAL_Z",
            "PRE-PINTADA": "MATERIAL_W",
            "FINA FRIA": "MATERIAL_V",
            # Adicione outros mapeamentos conforme necessário
        })

        df["Espessura"] = df["Espessura"].apply(
            lambda x: float(str(x).replace(".", "")) / 100 if isinstance(x, str) else x
        )
        transform_number(df, "Largura", int, 10)
        df["Comprimento"] = df["Comprimento"] * 1000
        transform_number(df, "Quantidade atual", float, 1000)
        df.rename(columns={"Descricao.1": "Grupo", "Descricao.2": "Setor"}, inplace=True)
        merge_df = df[["Codigo", "Und.Estoque", "Espessura", "Largura", "Comprimento", "Setor"]]
        df["Empresa"] = empresa
        return df, merge_df
    except Exception as e:
        raise Exception(f"Erro na transformação de produtos: {str(e)}")

def transform_producao(df, merge_df, empresa):
    """
    Transforma o DataFrame de produção.

    Args:
        df (pd.DataFrame): DataFrame de produção.
        merge_df (pd.DataFrame): DataFrame para merge.
        empresa (str): Nome da empresa.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        df = df[["Produto", "Data producao", "OP", "Descricao", "Lote", "Quantidade",
                 "Unidade", "Maquina", "Grupo de producao", "Responsavel pesagem", "Nome"]]
        df.dropna(subset=["Lote"], inplace=True)
        transform_date(df, "Data producao")
        df["Lote"] = df["Lote"].astype(int)
        transform_number(df, "Quantidade", float, 1000)
        df["Produto"] = df["Produto"].astype(int)
        df = pd.merge(df, merge_df, left_on="Produto", right_on="Codigo", how="left")
        df.drop(columns="Codigo", inplace=True)
        df.rename(columns={"Nome": "Empresa"}, inplace=True)

        # Regras de negócio para Empresa_B
        if empresa == "Empresa_A":
            mask = (
                df["Maquina"].str.contains("STZ") |
                df["Maquina"].str.startswith("COLAGEM") |
                df["Maquina"].str.startswith("CABINE") |
                df["Responsavel pesagem"].str.startswith("FUNCIONARIO_X")
            )
            df.loc[mask, "Empresa"] = "Empresa_B"
        return df
    except Exception as e:
        raise Exception(f"Erro na transformação de produção: {str(e)}")

def transform_faturamento(df, empresa):
    """
    Transforma o DataFrame de faturamento.

    Args:
        df (pd.DataFrame): DataFrame de faturamento.
        empresa (str): Nome da empresa.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        df = df[["Nota", "Pedido", "Razao Social", "Produto", "Descricao", "Vlr.Total Produtos",
                 "Peso Liquido", "Quantidade", "Und", "Valor unitario", "Total Liquido",
                 "Faturamento", "Cidade", "UF", "Nome", "Empresa", "Descricao.1",
                 "Descricao.2", "Espessura", "Largura", "% Comissao", "Nome.1"]]
        
        # Anonimiza valores de Descricao.1 e Descricao.2
        df["Descricao.1"] = df["Descricao.1"].replace({
            "CHAPA": "CATEGORIA_X",
            "BOBINA": "CATEGORIA_Y",
        })
        df["Descricao.2"] = df["Descricao.2"].replace({
            "GALVALUME": "MATERIAL_X",
            "ZINCADA": "MATERIAL_Y",
            "FINA QUENTE": "MATERIAL_Z",
            "PRE-PINTADA": "MATERIAL_W",
            "FINA FRIA": "MATERIAL_V",
        })

        df.dropna(subset=["Pedido"], inplace=True)
        transform_number(df, "Vlr.Total Produtos", float, 100)
        transform_number(df, "Peso Liquido", float, 1000)
        transform_number(df, "Quantidade", float, 10000)
        df["Quantidade"] = df["Quantidade"].round(3)
        transform_number(df, "Valor unitario", float, 10000)
        transform_number(df, "Total Liquido", float, 100)
        transform_date(df, "Faturamento")
        transform_number(df, "Largura", float, 10)
        df["Entrega"] = df["Cidade"] + "-" + df["UF"] + "-Brasil"
        df.drop(columns=["Cidade", "UF"], inplace=True)
        df["Empresa"] = empresa
        df["Valor por kg"] = df["Vlr.Total Produtos"] / df["Peso Liquido"]
        df["Peso por peças"] = df["Peso Liquido"] / df["Quantidade"]
        df.rename(columns={"Descricao.1": "Setor", "Descricao.2": "Grupo", "Nome.1": "Tipo entrega"}, inplace=True)

        produtos_empresa_a = ["CATEGORIA_A", "CATEGORIA_B", "CATEGORIA_C", "CATEGORIA_D", "CATEGORIA_E",
                             "CATEGORIA_F", "CATEGORIA_G", "CATEGORIA_H", "CATEGORIA_I",
                             "CATEGORIA_J", "CATEGORIA_K", "CATEGORIA_L", "CATEGORIA_M",
                             "CATEGORIA_N", "CATEGORIA_O", "CATEGORIA_P", "CATEGORIA_Q",
                             "CATEGORIA_R", "CATEGORIA_S"]
        df["Empresa de produção"] = df["Empresa"]
        df.loc[~df["Grupo"].isin(produtos_empresa_a), "Empresa de produção"] = "Empresa_B"

        carregamento_empresa_b = ["CLIENTE_X", "TRANSPORTADORA_Y", "TRANSPORTADORA_Z"]
        df.loc[df["Tipo entrega"].isin(carregamento_empresa_b), "Empresa de produção"] = "Empresa_C"

        df.loc[(df["Razao Social"].str.startswith("CLIENTE_W")) &
               (df["Faturamento"] > "2025-03-10") &
               (df["Grupo"] == "CATEGORIA_A"), "Empresa de produção"] = "Empresa_A"
        return df
    except Exception as e:
        raise Exception(f"Erro na transformação de faturamento: {str(e)}")

def transform_f9(df, empresa):
    """
    Transforma o DataFrame F9.

    Args:
        df (pd.DataFrame): DataFrame F9.
        empresa (str): Nome da empresa.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        df["Entrega"] = df["Cidade para Entrega"] + ", " + df["UF"] + ", BRASIL"
        df = df[["Pedido", "Situacao Aprovacao", "Descricao", "Entrega"]]
        df["Pedido"] = df["Pedido"].astype(str).str.replace(".", "")
        return df
    except Exception as e:
        raise Exception(f"Erro na transformação de F9: {str(e)}")

def transform_carteira(df, merge_df, empresa):
    """
    Transforma o DataFrame de carteira.

    Args:
        df (pd.DataFrame): DataFrame de carteira.
        merge_df (pd.DataFrame): DataFrame para merge.
        empresa (str): Nome da empresa.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        df.dropna(subset=["SKU"], inplace=True)
        df = df[["Emissao", "Pedido", "SKU", "Descricao Material", "Qtd. Pedida",
                 "Qtd. Em Aberto", "Qtde.Pecas", "Qtd.Disponivel", "Qt. Reservada",
                 "U.M", "Peso Liquido", "Valor Total", "Sit. Pedido",
                 "Dt. Aprovacao", "Razao Social Cliente", "Cidade", "UF",
                 "Razao Social Vendedor", "US", "Cor"]]
        df = df[df["Dt. Aprovacao"] != "/  /"]
        transform_date(df, "Emissao")
        transform_number(df, "Pedido", int, 1)
        transform_number(df, "Qtd. Em Aberto", float, 100)
        transform_number(df, "Qtd.Disponivel", float, 1000)
        transform_number(df, "Peso Liquido", float, 1000)
        transform_number(df, "Valor Total", float, 100)
        transform_date(df, "Dt. Aprovacao")
        df["Entrega"] = df["Cidade"] + "-" + df["UF"] + "-Brasil"
        df.drop(columns=["Cidade", "UF"], inplace=True)
        df = pd.merge(df, merge_df[["Codigo", "Grupo", "Espessura", "Setor"]], left_on="SKU", right_on="Codigo", how="left")
        df["US"] = df["US"].map({40: "Empresa_A", 1: "Empresa_B"})
        return df
    except Exception as e:
        raise Exception(f"Erro na transformação de carteira: {str(e)}")

def transform_carteira_additional(df, produtos_df):
    """
    Aplica transformações adicionais ao DataFrame de carteira.

    Args:
        df (pd.DataFrame): DataFrame de carteira.
        produtos_df (pd.DataFrame): DataFrame de produtos.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        df["Producao"] = df["US"]
        prod_empresa_b = ["CATEGORIA_T", "CATEGORIA_U", "CATEGORIA_V"]
        df.loc[df["Grupo"].isin(prod_empresa_b), "Producao"] = "Empresa_B"
        df.loc[(df["US"] == "Empresa_B") & (df["Grupo"] == "CATEGORIA_Y"), "Grupo"] = "CATEGORIA_P"
        df.loc[(df["Grupo"] == "CATEGORIA_A") & (df["Razao Social Cliente"].str.startswith("CLIENTE_W")), "Producao"] = "Empresa_A"

        df["Espessura real"] = df["Espessura"]
        tac = df[df["Grupo"] == "CATEGORIA_T"].copy()
        tac["Espessura real"] = tac["Descricao Material"].apply(
            lambda x: re.findall(r"(\d+\.\d+)", x) if isinstance(x, str) else []
        )
        tac = tac.explode("Espessura real")
        tac["Material"] = tac["Descricao Material"].apply(
            lambda x: re.findall(r"(PP|GL|ZN)", x) if isinstance(x, str) else []
        )
        tac = tac.explode("Material")
        tac = tac.groupby("Pedido", as_index=False).nth([0, 3])
        tac["Qtd. Em Aberto"] = tac["Qtd. Em Aberto"] / 2

        carteira2 = df[df["Grupo"] != "CATEGORIA_T"].copy()
        carteira2["Material"] = carteira2["Setor"]
        carteira2["Material"].replace({
            "MATERIAL_X": "GL", "MATERIAL_Y": "ZN", "MATERIAL_Z": "FQ",
            "MATERIAL_W": "PP", "MATERIAL_V": "FF", "ZINCADA POS-PIN": "ZN",
            "GALVALU POS-PIN": "GL"
        }, inplace=True)

        df = pd.concat([tac, carteira2]).sort_values(by="Pedido", ascending=True)
        df["Espessura real"] = pd.to_numeric(df["Espessura real"], errors="coerce")

        # Ajustes de espessura
        for grupo, old_val, new_val in [
            ("CATEGORIA_B", 0.38, 0.35), ("CATEGORIA_B", 0.43, 0.38), ("CATEGORIA_B", 0.50, 0.47), ("CATEGORIA_B", 0.65, 0.60),
            ("CATEGORIA_Y", 0.43, 0.40), ("CATEGORIA_Y", 0.50, 0.47), ("CATEGORIA_Y", 0.65, 0.60),
            ("CATEGORIA_F", 0.43, 0.40)
        ]:
            df.loc[(df["Grupo"].str.startswith(grupo)) & (df["Espessura real"] == old_val), "Espessura real"] = new_val

        df["Espessura real"] = df["Espessura real"].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
        df["Base"] = "B" + df["Material"].astype(str)
        for grupo in ["CATEGORIA_F", "CATEGORIA_N", "CATEGORIA_O"]:
            mask = df["Grupo"] == grupo
            df.loc[mask, "Base"] = "C" + df.loc[mask, "Material"].astype(str)
        df["Material"] = df["Base"] + " " + df["Espessura real"]
        df.loc[df["Setor"] == "CATEGORIA_I", "Material"] = "REVENDA"
        lcg = (df["Grupo"].str.startswith("CATEGORIA_N")) & (df["Espessura"] > 12.7)
        df.loc[lcg, "Material"] = df.loc[lcg, "Material"].str.replace("CFQ", "LCG")
        df = df.drop(columns="Base").reset_index(drop=True)

        # Processa produtos_cart
        produtos_cart = produtos_df[produtos_df["Grupo"].isin(["CATEGORIA_X", "CATEGORIA_Y"])].copy()
        produtos_cart["Material"] = ""
        produtos_cart.loc[produtos_cart["Grupo"] == "CATEGORIA_X", "Material"] = "C"
        produtos_cart.loc[produtos_cart["Grupo"] == "CATEGORIA_Y", "Material"] = "B"
        mask = produtos_cart["Material"] != ""
        produtos_cart.loc[mask & produtos_cart["Setor"].isin(["MATERIAL_Z"]), "Material"] += "FQ"
        produtos_cart.loc[mask & produtos_cart["Setor"].isin(["MATERIAL_Y"]), "Material"] += "ZN"
        produtos_cart.loc[mask & produtos_cart["Setor"].str.startswith("MATERIAL_X"), "Material"] += "GL"
        produtos_cart.loc[mask & produtos_cart["Setor"].isin(["MATERIAL_W"]), "Material"] += "PP"
        produtos_cart.loc[mask & produtos_cart["Setor"].isin(["MATERIAL_V"]), "Material"] += "FF"
        produtos_cart["Espessura"] = pd.to_numeric(produtos_cart["Espessura"], errors="coerce").apply(
            lambda x: f"{x:.2f}" if pd.notna(x) else ""
        )
        produtos_cart["Material"] = produtos_cart["Material"] + " " + produtos_cart["Espessura"]
        produtos_cart = produtos_cart[produtos_cart["Material"].str.match(r"^(CF|BF|BG|BZ|BP|LC)")]
        produtos_cart["Material"] = produtos_cart["Material"].str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
        df["Material"] = df["Material"].str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
        produtos_cart = produtos_cart[["Empresa", "Material", "Quantidade atual"]]
        produtos_cart.columns = ["Local do estoque", "Material", "Quantidade Estoque"]

        # Agrupa produtos_cart por empresa
        for emp in ["Empresa_A", "Empresa_B"]:
            temp_df = produtos_cart[produtos_cart["Local do estoque"] == emp]
            temp_df = temp_df.groupby(["Material", "Local do estoque"]).agg({"Quantidade Estoque": "sum"}).reset_index()
            df_temp = df[df["US"] == emp]
            df = df.drop(df[df["US"] == emp].index)
            df = pd.concat([df, pd.merge(df_temp, temp_df, on="Material", how="left")], axis=0)

        return df
    except Exception as e:
        raise Exception(f"Erro na transformação adicional de carteira: {str(e)}")