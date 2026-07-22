"""
Definições SQL do modelo dimensional (tabela fato + views de dimensão).

Separado do pipeline principal para facilitar manutenção: mudanças na
modelagem (nova view, ajuste de tipo de coluna, etc.) ficam isoladas aqui.
"""

# Criação da tabela fato
SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS fVendas (
    ID_Linha BIGINT PRIMARY KEY,
    Categoria TEXT, Cidade TEXT, Pais TEXT, ID_Cliente TEXT,
    Nome_Cliente TEXT, Desconto DOUBLE PRECISION, Mercado TEXT,
    Data_Pedido TIMESTAMP, ID_Pedido TEXT, Prioridade_Pedido TEXT,
    ID_Produto TEXT, Nome_Produto TEXT, Lucro_USD DOUBLE PRECISION,
    Quantidade BIGINT, Regiao TEXT, Vendas_USD DOUBLE PRECISION,
    Segmento TEXT, Data_Envio TIMESTAMP, Modo_Envio TEXT,
    Custo_Envio DOUBLE PRECISION, Estado TEXT, Sub_Categoria TEXT,
    Ano BIGINT, Mercado2 TEXT, N_Semana BIGINT,
    Tempo_Envio_Dias BIGINT, Vendas_BRL DOUBLE PRECISION,
    Margem_Lucro DOUBLE PRECISION,
    Lucro_BRL DOUBLE PRECISION, Custo_Envio_BRL DOUBLE PRECISION
);
"""

VIEWS_SQL = [
    """
    CREATE OR REPLACE VIEW dProduto AS
    SELECT ROW_NUMBER() OVER (ORDER BY ID_Produto, Nome_Produto) AS ID_Produto_SK,
    ID_Produto AS Codigo_Original, Nome_Produto, Categoria, Sub_Categoria
    FROM (SELECT DISTINCT ID_Produto, Nome_Produto, Categoria, Sub_Categoria FROM fVendas) AS subquery;
    """,
    
    """
    CREATE OR REPLACE VIEW dLocalidade AS
    SELECT ROW_NUMBER() OVER (ORDER BY f.Pais, f.Estado, f.Cidade) AS ID_Localidade,
    f.Pais, f.Estado, f.Cidade,
    (
        SELECT f2.Regiao FROM fVendas f2
        WHERE f2.Pais = f.Pais AND f2.Estado = f.Estado AND f2.Cidade = f.Cidade
        GROUP BY f2.Regiao
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS Regiao
    FROM fVendas f
    GROUP BY f.Pais, f.Estado, f.Cidade;
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