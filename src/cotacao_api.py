import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc
from urllib.parse import quote_plus

# Carregamento da chave para o banco de dados
load_dotenv()

def buscar_cotacao_dolar():
    print("° Consultando API do Banco Central do Brasil (Olinda)...")
    hoje = datetime.now()
    cotacao = None
    
    for i in range(5):
        data_busca = (hoje - timedelta(days=i)).strftime("%m-%d-%Y")
        url = (
            "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{data_busca}'"
            "&$top=100&$format=json"
        )

        try:
            resposta = requests.get(url, timeout=10)
            dados = resposta.json()
            resultados = dados.get('value', [])

            if resultados:
                
                # Pega a última cotação (fechamento ou última parcial do dia)
                cotacao = float(resultados[-1]['cotacaoVenda'])
                data_formatada = (hoje - timedelta(days=i)).strftime("%d/%m/%Y")
                print(f"° Cotação obtida via BACEN ({data_formatada}): R$ {cotacao:.4f}")
                return cotacao

        except Exception as e:
            print(f"Falha na requisição para a data {data_busca}: {e}")
            break


    print("Não foi possível obter a cotação recente. Usando valor padrão.")
    return 5.07

def conexao_bd():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    user_enc = quote_plus(user)
    password_enc = quote_plus(password)

    connection_url = (
        f"postgresql+psycopg2://{user_enc}:{password_enc}@{host}:{port}/{db_name}"
        f"?sslmode=require"
    )

    try:
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            print("Conexão com o Supabase (PostgreSQL) estabelecida!")
        return engine
    except exc.SQLAlchemyError as e:
        print(f"Erro ao conectar no banco: {e}")
        return None