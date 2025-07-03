# packages/jobs/utils/rastreio.py
from packages.database.connection import get_db_cursor

def registrar_status(distribuidora: str, ano: int, camada: str, status: str) -> None:
    """
    Insere ou atualiza uma linha em import_status para esta
    (distribuidora, ano, camada) com o novo status e data_execucao = NOW().
    """
    sql = """
    INSERT INTO import_status(distribuidora, ano, camada, status, data_execucao)
      VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT(distribuidora, ano, camada)
      DO UPDATE SET status = EXCLUDED.status,
                    data_execucao = EXCLUDED.data_execucao
    """
    with get_db_cursor(commit=True) as cur:
        cur.execute(sql, (distribuidora, ano, camada, status))
