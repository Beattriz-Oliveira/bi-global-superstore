import kagglehub
import shutil
import os

# Fazendo download temporario na pasta
print("Baixando Database")
path = kagglehub.dataset_download("anandaramg/global-superstore")

# Localiza o arquivo baixado
arquivos = os.listdir(path)
print(f"Arquivos encontrados: {arquivos}")

# Move o arquivo para a pasta "data"
for arquivo in arquivos:
    origem = os.path.join(path,arquivo)
    destino = os.path.join("data",arquivo)
    shutil.move(origem,destino)
    print(f"✅ {arquivo} movido com sucesso!")