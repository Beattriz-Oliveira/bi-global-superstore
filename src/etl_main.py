import pandas as pd
import numpy as np
import os
from utils import buscar_cotacao_dolar, conexao_bd
from sqlalchemy import text

def carregar_dados(nome_arquivo):
    
    caminho_arquivo = os.path.join("data", nome_arquivo)
    
    print(f"° Carregando Dados de {caminho_arquivo}")
    
    df =  pd.read_csv(caminho_arquivo, sep = '\t', encoding = 'utf-8')
    
    print("° Verificando valores nulos por coluna:")
    print(df.isnull().sum())
    
    return df
    
def transformar_dados(df,taxa):
    
    print("° Iniciando limpeza de dados ...")
    
    df.columns = [col.strip() for col in df.columns]
    
    # Mapeamento da coluna chinesa "记录数"
    for col in df.columns:
        if "è°°" in col or "记录数" in col:
            df = df.rename(columns={col: "N_Registros"})
    
    # Tradução dos cabeçalhos
    dicionario_traducao = {
        'Category': 'Categoria',
        'City': 'Cidade',
        'Country': 'Pais',
        'Customer ID': 'ID_Cliente',
        'Customer Name': 'Nome_Cliente',
	    'Discount': 'Desconto',
        'Market': 'Mercado',
        'Order Date': 'Data_Pedido',
        'Order ID': 'ID_Pedido',
        'Order Priority': 'Prioridade_Pedido',
        'Product ID': 'ID_Produto',
        'Product Name': 'Nome_Produto',
        'Profit': 'Lucro_USD',
        'Quantity': 'Quantidade',
        'Region': 'Regiao',
	    'Row ID': 'ID_Linha',
        'Sales': 'Vendas_USD',
        'Segment': 'Segmento',
        'Ship Date': 'Data_Envio',
        'Ship Mode': 'Modo_Envio',
	    'Shipping Cost': 'Custo_Envio',
        'State': 'Estado',
        'Sub-Category': 'Sub_Categoria',
	    'Year': 'Ano',
        'Market2': 'Mercado2',
	    'weeknum': 'N_Semana'
    }
    
    df = df.rename(columns=dicionario_traducao)
    
    # Datas e Nulos
    df['Data_Pedido'] = pd.to_datetime(df["Data_Pedido"])
    df['Data_Envio'] = pd.to_datetime(df["Data_Envio"])

    # Criaçãod de métricas
    df['Tempo_Envio_Dias'] = (df["Data_Envio"] - df["Data_Pedido"]).dt.days
    df['Vendas_BRL'] = df['Vendas_USD'] * taxa
    df['Lucro_BRL'] = df['Lucro_USD'] * taxa
    df['Custo_Envio_BRL'] = df['Custo_Envio'] * taxa
    df['Margem_Lucro'] = df['Lucro_USD'] / df['Vendas_USD'].replace(0, np.nan)
    
    # Tradução dos dados importantes
    mapeamento_conteudo = {
        'Categoria': {
            'Office Supplies': 'Material de Escritório',
            'Furniture': 'Móveis',
            'Technology': 'Tecnologia'
        },
        'Sub_Categoria': {
            'Paper': 'Papel', 'Binders': 'Fichários', 'Art': 'Arte',
            'Envelopes': 'Envelopes', 'Fasteners': 'Fixadores', 'Labels': 'Etiquetas',
            'Storage': 'Armazenamento', 'Supplies': 'Suprimentos',
            'Appliances': 'Eletrodomésticos', 'Chairs': 'Cadeiras', 'Tables': 'Mesas',
            'Bookcases': 'Estantes', 'Phones': 'Telefones', 'Accessories': 'Acessórios',
            'Copiers': 'Copiadoras', 'Machines': 'Máquinas'
        },
        'Prioridade_Pedido': {
            'High': 'Alta', 'Medium': 'Média', 'Low': 'Baixa', 'Critical': 'Crítica'
        },
        'Modo_Envio': {
            'Standard Class': 'Classe Econômica', 'Second Class': 'Segunda Classe',
            'First Class': 'Primeira Classe', 'Same Day': 'Mesmo Dia'
        },
        'Segmento': {
            'Consumer': 'Consumidor', 'Corporate': 'Corporativo', 'Home Office': 'Home Office'
        },
        'Mercado': {
            'US': 'EUA', 'EU': 'Europa', 'APAC': 'Ásia-Pacífico',
            'LATAM': 'América Latina', 'Africa': 'África', 'EMEA': 'Oriente Médio/África'
        },
        'Mercado2': {
            'North America': 'América do Norte', 'Oceania': 'Oceania',
            'Central America': 'América Central', 'South America': 'América do Sul',
            'Europe': 'Europa', 'Asia': 'Ásia', 'Africa': 'África'
        },
        'Regiao': {
            'West': 'Oeste', 'East': 'Leste', 'Central': 'Central', 'South': 'Sul',
            'North': 'Norte', 'Caribbean': 'Caribe', 'Southeast Asia': 'Sudeste Asiático',
            'Oceania': 'Oceania', 'Central Asia': 'Ásia Central', 'North Asia': 'Norte da Ásia'
        },
        'Pais': {
            'United States': 'Estados Unidos', 'Brazil': 'Brasil', 'France': 'França',
            'Germany': 'Alemanha', 'United Kingdom': 'Reino Unido', 'China': 'China',
            'Australia': 'Austrália', 'Mexico': 'México', 'Canada': 'Canadá'
        }
    }
    
    for coluna, dicionario in mapeamento_conteudo.items():
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

def executar_etl():
    
    # Caminhos
    arquivo_input = "Global Superstore.txt"
    arquivo_output = os.path.join("data", "vendas_Processadas.csv")
    
    # Pipeline
    try:
        # Extração
        df_raw = carregar_dados(arquivo_input)
        taxa = buscar_cotacao_dolar()
        
        # Transformação
        df_final = transformar_dados(df_raw,taxa)
        
        # Validação
        validar_consistencia(df_final)
        
        # Carga local
        df_final.to_csv(arquivo_output, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ Sucesso! Arquivo salvo em: {arquivo_output}")
        
        # Carga Nuvem
        print ("° Preparando banco na nuvem (Aiven)...")
        engine = conexao_bd()
        
        # Declarando a primary key
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS fVendas (
            ID_Linha BIGINT PRIMARY KEY,
            Categoria TEXT, Cidade TEXT, Pais TEXT, ID_Cliente TEXT,
            Nome_Cliente TEXT, Desconto DOUBLE, Mercado TEXT,
            Data_Pedido DATETIME, ID_Pedido TEXT, Prioridade_Pedido TEXT,
            ID_Produto TEXT, Nome_Produto TEXT, Lucro_USD DOUBLE,
            Quantidade BIGINT, Regiao TEXT, Vendas_USD DOUBLE,
            Segmento TEXT, Data_Envio DATETIME, Modo_Envio TEXT,
            Custo_Envio DOUBLE, Estado TEXT, Sub_Categoria TEXT,
            Ano BIGINT, Mercado2 TEXT, N_Semana BIGINT,
            Tempo_Envio_Dias BIGINT, Vendas_BRL DOUBLE, Margem_Lucro DOUBLE,
            Lucro_BRL DOUBLE, Custo_Envio_BRL DOUBLE
        );
        """
        
        # Criando Views
        views_sql = [
            """
            CREATE OR REPLACE VIEW dProduto AS 
            SELECT ROW_NUMBER() OVER (ORDER BY ID_Produto, Nome_Produto) AS ID_Produto_SK,
            ID_Produto AS Codigo_Original, Nome_Produto, Categoria, Sub_Categoria
            FROM (SELECT DISTINCT ID_Produto, Nome_Produto, Categoria, Sub_Categoria FROM fVendas) AS subquery;
            """,
            """
            CREATE OR REPLACE VIEW dLocalidade AS 
            SELECT ROW_NUMBER() OVER (ORDER BY Pais, Estado, Cidade) AS ID_Localidade,
            Pais, Estado, Regiao, Cidade
            FROM (SELECT DISTINCT Pais, Estado, Regiao, Cidade FROM fVendas) AS subquery;
            """,
            """
            CREATE OR REPLACE VIEW dCliente AS 
            SELECT DISTINCT ID_Cliente, Nome_Cliente, Segmento FROM fVendas;
            """,
            """
            CREATE OR REPLACE VIEW fVendas_Final AS 
            SELECT f.*, l.ID_Localidade, p.ID_Produto_SK
            FROM fVendas f
            LEFT JOIN dLocalidade l ON f.Pais = l.Pais AND f.Estado = l.Estado AND f.Cidade = l.Cidade
            LEFT JOIN dProduto p ON f.ID_Produto = p.Codigo_Original AND f.Nome_Produto = p.Nome_Produto;
            """
        ]
        
        with engine.begin() as con:
            print("° Limpando tabela antiga ...")
            con.execute(text("DROP TABLE IF EXISTS fVendas;"))
            
            print("° Criando nova tabela...")
            con.execute(text(sql_create_table))

            print("° Enviando dados (fVendas)...")
            # Passamos a conexão 'con' diretamente para o to_sql
            df_final.to_sql(name="fVendas", con=con, if_exists='append', index=False)
            
            print("° Criando Views...")
            for sql in views_sql:
                con.execute(text(sql))
            
        print("✅ Sucesso! Dados e Modelagem prontos na nuvem.")
        
    except FileNotFoundError:
        print(f"❌ Erro: O arquivo {arquivo_input} não foi encontrado na pasta data/")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    executar_etl()