import sqlite3
import traceback
from datetime import datetime
from typing import Optional
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

DB_ENVIOS = DATA_DIR / "crm_envios.sqlite"
DB_ERROS = DATA_DIR / "crm_erros.sqlite"

def init_db_envios(db_path: str = DB_ENVIOS) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS envios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cpf_cnpj TEXT,                -- cpf/cnpj limpo (se existir)
                codigo_cliente TEXT NOT NULL, -- código do cliente no CRM
                data_local TEXT NOT NULL,     -- YYYY-MM-DD (data local da máquina)
                created_at TEXT NOT NULL,     -- ISO timestamp local
                pedido_mag TEXT               -- pedido (se existir)
            )
        """)
        # Garante "no máximo 1 envio por cliente por dia"
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_envio_cliente_dia
            ON envios (codigo_cliente, data_local)
        """)
        conn.commit()

def init_db_erros(db_path: str = DB_ERROS) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS erros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cpf_cnpj TEXT,                -- cpf/cnpj limpo (se existir)
                codigo_cliente TEXT,          -- se já tiver
                created_at TEXT NOT NULL,     -- ISO timestamp local
                pedido_mag TEXT,              -- pedido (se existir)
                origem TEXT,                  -- onde falhou (ex: 'aut/cliente', 'criar_ocorrencia', etc.)
                error_type TEXT,
                error_message TEXT,
                traceback TEXT
            )
        """)
        conn.commit()

def cliente_ja_recebeu_hoje(codigo_cliente: str, data_hoje: str, db_path: str = DB_ENVIOS) -> bool:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "SELECT 1 FROM envios WHERE codigo_cliente = ? AND data_local = ? LIMIT 1",
            (codigo_cliente, data_hoje),
        )
        return cur.fetchone() is not None

def registrar_envio(
        cpf_cnpj: str, 
        codigo_cliente: str, 
        data_hoje: str, 
        pedido_mag: Optional[str], 
        db_path: str = DB_ENVIOS
) -> None:
    # Se já existir (unique), não insere de novo
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO envios (cpf_cnpj, codigo_cliente, data_local, created_at, pedido_mag)
            VALUES (?, ?, ?, ?, ?)
            """,
            (cpf_cnpj, codigo_cliente, data_hoje, datetime.now().isoformat(timespec="seconds"), pedido_mag),
        )
        conn.commit()

def registrar_erro(
    origem: str,
    exc: Exception,
    cpf_cnpj: Optional[str] = None,
    codigo_cliente: Optional[str] = None,
    pedido_mag: Optional[str] = None,
    db_path: str = DB_ERROS,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO erros (
                cpf_cnpj, codigo_cliente, created_at, pedido_mag, origem,
                error_type, error_message, traceback
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cpf_cnpj,
                codigo_cliente,
                datetime.now().isoformat(timespec="seconds"),
                pedido_mag,
                origem,
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
            ),
        )
        conn.commit()