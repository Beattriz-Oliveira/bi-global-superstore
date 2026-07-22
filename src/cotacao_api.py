import os
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc
from urllib.parse import quote_plus

# Carregamento da chave
load_dotenv()

def buscar_cotacao_dolar():

    print("° Consultando ExchangeRate-API...")

    api = os.getenv("API_Key")
    url = f"https://v6.exchangerate-api.com/v6/{api}/pair/USD/BRL"

    try:
        resposta = requests.get(url)
        dados = resposta.json()

        if dados.get('result') == 'success':
            cotacao = float(dados['conversion_rate'])

            print(f"° Cotação obtida via API: R$ {cotacao:.2f}")

            return cotacao

        else:
            print(f"Erro na API: {dados.get('error-type')}")
            return 5.50

    except Exception as e:
        print(f"Falha na requisição: {e}")
        return 5.50

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