import pandas as pd
import numpy as np
import os
from cotacao_api import buscar_cotacao_dolar, conexao_bd
from traducoes import DICIONARIO_COLUNAS, MAPEAMENTO_CONTEUDO
from views_sql import SQL_CREATE_TABLE, VIEWS_SQL
from sqlalchemy import text

def carregar_dados(nome_arquivo):

    caminho_arquivo = os.path.join("data", nome_arquivo)

    print(f"° Carregando Dados de {caminho_arquivo}")

    df = pd.read_csv(caminho_arquivo, sep='\t', encoding='utf-8')

    print("° Verificando valores nulos por coluna:")
    print(df.isnull().sum())

    return df

def transformar_dados(df, taxa):

    print("° Iniciando limpeza de dados ...")

    df.columns = [col.strip() for col in df.columns]

    # Mapeamento da coluna chinesa "记录数"
    for col in df.columns:
        if "è°°" in col or "记录数" in col:
            df = df.rename(columns={col: "N_Registros"})

    df = df.rename(columns=DICIONARIO_COLUNAS)

    # Datas e Nulos
    df['Data_Pedido'] = pd.to_datetime(df["Data_Pedido"])
    df['Data_Envio'] = pd.to_datetime(df["Data_Envio"])

    # Criação de métricas
    df['Tempo_Envio_Dias'] = (df["Data_Envio"] - df["Data_Pedido"]).dt.days
    df['Vendas_BRL'] = df['Vendas_USD'] * taxa
    df['Lucro_BRL'] = df['Lucro_USD'] * taxa
    df['Custo_Envio_BRL'] = df['Custo_Envio'] * taxa
    df['Margem_Lucro'] = df['Lucro_USD'] / df['Vendas_USD'].replace(0, np.nan)

    # Tradução dos valores de conteúdo
    for coluna, dicionario in MAPEAMENTO_CONTEUDO.items():
        if coluna in df.columns:
            df[coluna] = df[coluna].astype(str).str.strip().map(dicionario).fillna(df[coluna])

    colunas_banco = [
        'ID_Linha', 'Categoria', 'Cidade', 'Pais', 'ID_Cliente', 'Nome_Cliente',
        'Desconto', 'Mercado', 'Data_Pedido', 'ID_Pedido',
        'Prioridade_Pedido', 'ID_Produto', 'Nome_Produto', 'Lucro_USD',
        'Quantidade', 'Regiao', 'Vendas_USD', 'Segmento', 'Data_Envio',
        'Modo_Envio', 'Custo_Envio', 'Estado', 'Sub_Categoria', 'Ano',
        'Mercado2', 'N_Semana', 'Tempo_Envio_Dias', 'Vendas_BRL', 'Margem_Lucro',
        'Lucro_BRL', 'Custo_Envio_BRL'
    ]

    return df[colunas_banco]

def validar_consistencia(df):

    print("° Validando consistência das informações ...")

    atraso_medio = df['Tempo_Envio_Dias'].mean()
    print(f"° Insight: O tempo médio de envio é de {atraso_medio:.2f} dias.")

    if (df['Vendas_USD'] < 0).any():
        print("⚠️ Atenção: Detectadas vendas negativas!")
    else:
        print("✅ Dados de vendas consistentes.")

    # Checagem de fan-out: cidades com mais de uma Região associada.
    cidades_conflito = df.groupby(['Pais', 'Estado', 'Cidade'])['Regiao'].nunique()
    n_conflitos = (cidades_conflito > 1).sum()
    if n_conflitos > 0:
        print(f"⚠️ Atenção: {n_conflitos} combinações Pais/Estado/Cidade têm mais "
              f"de uma Região associada. A view dLocalidade trata isso escolhendo "
              f"a Região mais frequente (ver views_sql.py).")
    else:
        print("✅ Nenhuma inconsistência de Região por cidade encontrada.")

def executar_etl():

    arquivo_input = "Global Superstore.txt"
    arquivo_output = os.path.join("data", "vendas_Processadas.csv")

    try:
        # Extração
        df_raw = carregar_dados(arquivo_input)
        taxa = buscar_cotacao_dolar()

        # Transformação
        df_final = transformar_dados(df_raw, taxa)

        # Validação
        validar_consistencia(df_final)

        # Carga local
        df_final.to_csv(arquivo_output, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ Sucesso! Arquivo salvo em: {arquivo_output}")

        # Carga Nuvem (Supabase / PostgreSQL)
        print("° Preparando banco na nuvem (Supabase)...")
        engine = conexao_bd()

        with engine.begin() as con:
            print("° Limpando tabela antiga ...")
            con.execute(text("DROP TABLE IF EXISTS fVendas CASCADE;"))

            print("° Criando nova tabela...")
            con.execute(text(SQL_CREATE_TABLE))

            print("° Enviando dados (fVendas)...")
            df_final.to_sql(name="fVendas", con=con, if_exists='append', index=False)

            print("° Criando Views...")
            for sql in VIEWS_SQL:
                con.execute(text(sql))

        print("✅ Sucesso! Dados e Modelagem prontos na nuvem.")

    except FileNotFoundError:
        print(f"❌ Erro: O arquivo {arquivo_input} não foi encontrado na pasta data/")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    executar_etl()