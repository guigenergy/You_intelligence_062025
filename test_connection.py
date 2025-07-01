# test_connection.py
import os
from packages.database.connection import get_db_cursor
from dotenv import load_dotenv

load_dotenv()

def testar_conexao():
    schema = os.getenv("DB_SCHEMA", "plead")
    # solicita cursor sem dicionário
    with get_db_cursor(dict_cursor=False) as cur:
        # 1) Verifica o search_path ativo
        cur.execute("SHOW search_path")
        sp = cur.fetchone()[0]
        print("🔍 search_path atual:", sp)

        # 2) Conta quantas linhas existem em uma tabela do schema
        cur.execute(f"SELECT COUNT(*) FROM {schema}.unidade_consumidora")
        total = cur.fetchone()[0]
        print(f"📊 Total de registros em {schema}.unidade_consumidora:", total)

if __name__ == "__main__":
    testar_conexao()
