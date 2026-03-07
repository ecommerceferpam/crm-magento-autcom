import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional
from streamlit_autorefresh import st_autorefresh
import sqlite3
import pandas as pd

import streamlit as st

# ========= Ajuste caminhos =========
BASE = Path(__file__).resolve().parent
RUNTIME = BASE / "runtime"
DATA = BASE / "data"
RUNTIME.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)

CONTROL_PATH = BASE / "control.json"

PID_PATH = RUNTIME / "bot.pid"
STOP_FLAG = RUNTIME / "stop.flag"
BOT_LOG = RUNTIME / "bot.log"
ERR_LOG = RUNTIME / "errors.log"
STATUS_PATH = RUNTIME / "status.json"

DB_ENVIOS = DATA / "crm_envios.sqlite"
DB_ERROS = DATA / "crm_erros.sqlite"

BOT_CMD = [sys.executable, "-u", str(BASE / "bot_worker.py")]  # <- troque se precisar

DEFAULT_CONTROL: Dict[str, Any] = {
    "poll_seconds": 20,
    "modules": {
        "pedidos_cpf": {"enabled": True, "min_value": 2000.0},
        "pedidos_cnpj": {"enabled": True, "min_value": 0.0},
        "palavras_chave": {"enabled": False, "min_value": 0.0, "keywords": ["fraude", "chargeback"]},
        "carrinhos_abandonados": {"enabled": False, "min_value": 0.0},
    },
}


def load_control() -> Dict[str, Any]:
    if not CONTROL_PATH.exists():
        save_control(DEFAULT_CONTROL)
        return DEFAULT_CONTROL
    try:
        return json.loads(CONTROL_PATH.read_text(encoding="utf-8"))
    except Exception:
        # se quebrar, recria
        save_control(DEFAULT_CONTROL)
        return DEFAULT_CONTROL


def save_control(control: Dict[str, Any]) -> None:
    CONTROL_PATH.write_text(json.dumps(control, indent=2, ensure_ascii=False), encoding="utf-8")


def read_pid() -> Optional[int]:
    if not PID_PATH.exists():
        return None
    try:
        return int(PID_PATH.read_text().strip())
    except Exception:
        return None


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def bot_status() -> Dict[str, Any]:
    pid = read_pid()
    if pid is None:
        return {"running": False, "pid": None}
    alive = is_process_alive(pid)
    return {"running": alive, "pid": pid if alive else None}


def read_status() -> dict:
    if not STATUS_PATH.exists():
        return {}
    try:
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fmt_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"


def start_bot() -> None:
    # evita start duplicado
    status = bot_status()
    if status["running"]:
        return

    # limpa flag de stop
    if STOP_FLAG.exists():
        STOP_FLAG.unlink(missing_ok=True)

    # abre arquivos de log
    bot_log_f = open(BOT_LOG, "a", encoding="utf-8")
    err_log_f = open(ERR_LOG, "a", encoding="utf-8")

    # inicia processo
    proc = subprocess.Popen(
        BOT_CMD,
        stdout=bot_log_f,
        stderr=err_log_f,
        cwd=str(BASE),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    PID_PATH.write_text(str(proc.pid), encoding="utf-8")


def stop_bot_graceful(timeout_sec: int = 15) -> None:
    status = bot_status()
    if not status["running"]:
        PID_PATH.unlink(missing_ok=True)
        return

    pid = status["pid"]
    assert pid is not None

    # pede stop gracioso
    STOP_FLAG.write_text("1", encoding="utf-8")

    # espera
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        if not is_process_alive(pid):
            PID_PATH.unlink(missing_ok=True)
            STOP_FLAG.unlink(missing_ok=True)
            return
        time.sleep(0.5)

    # força encerramento se não parou
    force_kill(pid)


def force_kill(pid: int) -> None:
    try:
        if os.name == "nt":
            os.kill(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    time.sleep(0.8)
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        pass
    PID_PATH.unlink(missing_ok=True)
    STOP_FLAG.unlink(missing_ok=True)


def tail_file(path: Path, max_lines: int = 200) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception as e:
        return f"[erro lendo {path.name}] {e}"


# ======== UI ========

st.set_page_config(page_title="CRM E-COMMERCE", layout="wide")
status = bot_status()
interval_ms = 1000 if status["running"] else 5000 # refresh dinâmico: mais rápido quando o bot está rodando

st.markdown("""
<style>
.block-container { padding-top: 1.2rem; }
/* Botão Ligar (verde) */
.st-key-ligar div[data-testid="stButton"] button[kind="primary"] {
    background-color: #2eb0db0d !important;
    border-color: #2eb0db !important;
}
.st-key-desligar div[data-testid="stButton"] button[kind="secondary"] {
    background-color: #de07070d !important;
    border-color: #de0707 !important;
}
.st-key-refresh { display: none; }
</style>
""", unsafe_allow_html=True)


left, right = st.columns([1.2, 1], vertical_alignment="center")

with left:
    st.markdown("## CRM E-COMMERCE")
    if status["running"]:
        st.caption(f"🟢 Rodando (PID {status['pid']})")
    else:
        st.caption("🔴 Desligado")

with right:
    b1, b2, b3, b4 = st.columns([1, 1, 1.2, 1.2], vertical_alignment="center")

    with b1:
        if st.button("Ligar", width="stretch", type="primary", disabled=status["running"], key="ligar"):
            with open(BOT_LOG, "w", encoding="utf-8") as f:
                f.write("")
            start_bot()
            st.rerun()

    with b2:
        if st.button("Desligar", width="stretch", type="secondary", disabled=not status["running"], key="desligar"):
            stop_bot_graceful()
            st.rerun()

    with b3:
        if st.button("Forçar parada", width="stretch", disabled=not status["running"]):
            pid = status["pid"]
            if pid:
                force_kill(pid)
            st.rerun()

    with b4:
        auto = st.checkbox(f"Auto ({interval_ms//1000}s)", value=True)

if auto:
    st_autorefresh(interval=interval_ms, key="refresh")


st_status = read_status()
state = st_status.get("state")
next_at = st_status.get("next_scan_at")
poll_seconds = int(st_status.get("poll_seconds", 0))
now = time.time()

progress_slot = st.empty()   # reserva o espaço da barra (não some)
caption_slot = st.empty()    # opcional: reserva o texto também

# ... calcula state/next_at/poll_seconds/now ...

if state in ("sleeping", "idle") and isinstance(next_at, (int, float)) and poll_seconds > 0:
    remaining = int(next_at - now)
    caption_slot.caption(f"Próxima varredura em: **{fmt_mmss(remaining)}**")
    progress = 1.0 - (remaining / poll_seconds)
    progress_slot.progress(min(max(progress, 0.0), 1.0))

elif state == "scanning":
    started = st_status.get("scan_started_at")
    if isinstance(started, (int, float)):
        caption_slot.caption(f"Varredura em andamento há: **{fmt_mmss(int(now - started))}**")
    else:
        caption_slot.caption("Varredura em andamento…")
    progress_slot.progress(1.0)  # barra “cheia” durante scanning (ou 0.0 se preferir)

else:
    caption_slot.caption("Timer indisponível (bot parado ou status ainda não gerado).")
    progress_slot.progress(0.0)  # barra “vazia” quando não tiver timer


tabs = st.tabs(["Envios", "Erros", "Configurações", "Terminal"])

def load_df(db_path, query: str, params=()):
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=params)

# checagem de existência
missing = []
if not DB_ENVIOS.exists():
    missing.append(f"DB_ENVIOS")
if not DB_ERROS.exists():
    missing.append(f"DB_ERROS")


# ----------- ENVIOS -----------
with tabs[0]:

    if "DB_ENVIOS" in missing:
        for m in missing:
            if m == "DB_ENVIOS":
                st.warning(f"O banco de dados {m} não foi encontrado, verifique.")
    else:
        st.subheader("Envios")

        q = st.text_input("Buscar (cpf_cnpj, codigo_cliente, pedido_mag)", "", key="q_envios")
        limit = st.number_input("Limite", 10, 5000, 500, 10, key="lim_envios")

        try:
            if q.strip():
                like = f"%{q.strip()}%"
                df = load_df(
                    DB_ENVIOS,
                    """
                    SELECT id, cpf_cnpj, codigo_cliente, data_local, created_at, pedido_mag
                    FROM envios
                    WHERE cpf_cnpj LIKE ? OR codigo_cliente LIKE ? OR pedido_mag LIKE ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (like, like, like, int(limit)),
                )
            else:
                df = load_df(
                    DB_ENVIOS,
                    """
                    SELECT id, cpf_cnpj, codigo_cliente, data_local, created_at, pedido_mag
                    FROM envios
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                )

            st.dataframe(df, width="stretch", hide_index=True)

        except Exception as e:
            st.error(f"Erro ao ler envios: {e}")

# ----------- ERROS ------------
with tabs[1]:

    if "DB_ERROS" in missing:
        for m in missing:
            if m == "DB_ERROS":
                st.warning(f"O banco de dados {m} não foi encontrado, verifique.")
    else:
        # ---------------- ERROs ----------------
        st.subheader("Erros")

        q = st.text_input(
            "Buscar (cpf_cnpj, codigo_cliente, pedido_mag, origem, mensagem)",
            "",
            key="q_erros",
        )
        limit = st.number_input("Limite", 10, 5000, 500, 10, key="lim_erros")

        try:
            if q.strip():
                like = f"%{q.strip()}%"
                df = load_df(
                    DB_ERROS,
                    """
                    SELECT id, cpf_cnpj, codigo_cliente, created_at, pedido_mag, origem, error_type, error_message
                    FROM erros
                    WHERE cpf_cnpj LIKE ?
                        OR codigo_cliente LIKE ?
                        OR pedido_mag LIKE ?
                        OR origem LIKE ?
                        OR error_message LIKE ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (like, like, like, like, like, int(limit)),
                )
            else:
                df = load_df(
                    DB_ERROS,
                    """
                    SELECT id, cpf_cnpj, codigo_cliente, created_at, pedido_mag, origem, error_type, error_message
                    FROM erros
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                )

            st.dataframe(df, width="stretch", hide_index=True)

        except Exception as e:
            st.error(f"Erro ao ler erros: {e}")

        with st.expander("Ver traceback (últimos 10)"):
            try:
                tb = load_df(
                    DB_ERROS,
                    """
                    SELECT id, created_at, origem, error_message, traceback
                    FROM erros
                    ORDER BY id DESC
                    LIMIT 10
                    """,
                )
                st.dataframe(tb, width="stretch", hide_index=True, height=320)
            except Exception as e:
                st.error(f"Erro ao carregar traceback: {e}")

# ------- CONFIGURAÇÕES --------
with tabs[2]:
    control = load_control()

    st.markdown("### Configuração da varredura")
    left, right = st.columns([1.2, 1])

    with left:
        poll = st.number_input(
            "Intervalo entre varreduras (segundos)",
            min_value=60,
            max_value=3600,
            value=int(control.get("poll_seconds", 60)),
            step=5,
        )
        control["poll_seconds"] = int(poll)

        st.divider()
        mods = control.get("modules", {})

        def module_box(key: str, label: str, has_keywords: bool = False) -> None:
            with st.expander(label, expanded=False):
                enabled = st.checkbox(
                    "Ativo",
                    value=bool(mods.get(key, {}).get("enabled", True)),
                    key=f"{key}_en",
                )

                minv = st.number_input(
                    "Valor mínimo",
                    min_value=0.0,
                    value=float(mods.get(key, {}).get("min_value", 0.0)),
                    step=10.0,
                    key=f"{key}_min",
                )

                mods.setdefault(key, {})
                mods[key]["enabled"] = bool(enabled)
                mods[key]["min_value"] = float(minv)

                if has_keywords:
                    raw = st.text_area(
                        "Palavras-chave (uma por linha)",
                        value="\n".join(mods.get(key, {}).get("keywords", [])),
                        height=120,
                        key=f"{key}_kw",
                    )
                    kws = [x.strip() for x in raw.splitlines() if x.strip()]
                    mods[key]["keywords"] = kws

        module_box("pedidos_cpf", "Pedidos CPF")
        module_box("pedidos_cnpj", "Pedidos CNPJ")
        module_box("palavras_chave", "Pedidos com palavras-chave", has_keywords=True)
        module_box("carrinhos_abandonados", "Carrinhos abandonados")

        control["modules"] = mods

        if st.button("Salvar configuração", type="primary"):
            save_control(control)
            st.success("Configuração salva em control.json")

    with right:
        st.caption("Preview")
        st.code(json.dumps(control, indent=2, ensure_ascii=False), language="json")

# ---------- TERMINAL ----------
with tabs[3]:
    c1, c2 = st.columns(2)
    with c1:

        st.session_state["stdout_box"] = tail_file(BOT_LOG, max_lines=250) or "(sem logs ainda)"
        
        st.text_area(
            label="Saída do bot (stdout)",
            height=420,
            disabled=True,
            key="stdout_box",
        )

        if st.button("Limpar", key="clear_stdout"):
            with open(BOT_LOG, "w", encoding="utf-8") as f:
                f.write("")
            st.rerun()

    with c2:

        st.session_state["stderr_box"] = tail_file(ERR_LOG, max_lines=250) or "(sem erros ainda)"

        st.text_area(
            label="Erros (stderr)",
            height=420,
            disabled=True,
            key="stderr_box",
        )


        if st.button("Limpar", key="clear_stderr"):
            with open(ERR_LOG, "w", encoding="utf-8") as f:
                f.write("")
            st.rerun()
