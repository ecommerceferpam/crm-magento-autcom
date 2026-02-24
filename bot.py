import json
import time
from pathlib import Path
from resources.utils import abrir_crm_carrinho
from resources import database as db, orders, utils as ut, carts

def _base_dir() -> Path:
    # se este arquivo estiver na raiz do projeto, BASE = raiz
    # se estiver em outra pasta, ajuste parent/parent...
    return Path(__file__).resolve().parent

BASE = _base_dir()
CONTROL_PATH = BASE / "control.json"
STOP_FLAG = BASE / "runtime" / "stop.flag"
STATUS_PATH = BASE / "runtime" / "status.json"

def write_status(**fields):
    STATUS_PATH.parent.mkdir(exist_ok=True)
    data = {}
    if STATUS_PATH.exists():
        try:
            data = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    data.update(fields)
    STATUS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

DEFAULT_CONTROL = {
    "poll_seconds": 60,  # 1 min
    "modules": {
        "pedidos_cpf": {"enabled": True, "min_value": 0.0},
        "pedidos_cnpj": {"enabled": True, "min_value": 0.0},
        "palavras_chave": {"enabled": True, "min_value": 0.0, "keywords": []},
        "carrinhos_abandonados": {"enabled": True, "min_value": 0.0},
    },
}

def load_control() -> dict:
    if not CONTROL_PATH.exists():
        CONTROL_PATH.write_text(json.dumps(DEFAULT_CONTROL, indent=2, ensure_ascii=False), encoding="utf-8")
        return DEFAULT_CONTROL

    try:
        return json.loads(CONTROL_PATH.read_text(encoding="utf-8"))
    except Exception:
        # se o json estiver corrompido, volta pro padrão
        CONTROL_PATH.write_text(json.dumps(DEFAULT_CONTROL, indent=2, ensure_ascii=False), encoding="utf-8")
        return DEFAULT_CONTROL

def stop_requested() -> bool:
    return STOP_FLAG.exists()

def main() -> None:
    # Inicializa DBs (pode chamar sempre)
    db.init_db_envios()
    db.init_db_erros()

    print("Iniciando nova execucao --------------------------")

    while True:
        # parada graciosa (pedida pelo Streamlit)
        if stop_requested():
            print(f"[{ut.hoje_iso()} {ut.hora_agora()}] Stop solicitado. Encerrando com seguranca.")
            write_status(state="stopped")
            break

        cfg = load_control()
        poll_seconds = max(5, int(cfg.get("poll_seconds", 300)))
        modules = cfg.get("modules", {})

        try:
            t_start = time.time()
            write_status(
                state="scanning",
                scan_started_at=t_start,
            )

            print(f"[{ut.hoje_iso()} {ut.hora_agora()}] Iniciando varredura")

            orders.processar_varredura(
                janela_de_minutos=max(1, (poll_seconds+60) // 60),
                modules=modules,
            )

            cfg_cart = modules.get("carrinhos_abandonados", {})
            cart_enabled = bool(cfg_cart.get("enabled", False))

            if cart_enabled:
                cart_min = float(cfg_cart.get("min_value", 0.0))

                try:
                    lista_carts = carts.listar_carrinhos_abandonados()
                except Exception as e:
                    db.registrar_erro(origem="buscar_carrinhos_abandonados", exc=e)

                if lista_carts.get("ok"):
                    carrinhos = lista_carts.get("abandonados")

                    data_hoje = ut.hoje_iso()

                    for c in carrinhos:
                        email = (c.get("email") or "").strip().lower()
                        try:
                            valor = float(c.get("valor_total", 0.0))
                            if valor < cart_min:
                                continue

                            if not email:
                                raise ValueError("Carrinho sem email")

                            # ideal: usar um ID real de cliente/carrinho
                            if db.cliente_ja_recebeu_hoje(codigo_cliente=email, data_hoje=data_hoje):
                                continue

                            items = c.get("items") or []
                            phone = (c.get("phone") or None)
                            abrir_crm_carrinho(items=items, valor=valor, email=email, phone=phone, data=data_hoje)

                        except Exception as e:
                            db.registrar_erro(
                                origem="processar_carrinho_abandonado",
                                exc=e,
                                cpf_cnpj=None,
                                codigo_cliente=email,
                                pedido_mag=None,
                            )


        except Exception as e:
            db.registrar_erro(origem="loop_varredura", exc=e)

        t_end = time.time()
        write_status(
            state="idle",
            scan_finished_at=t_end,
        )


        next_scan_at = time.time() + poll_seconds
        write_status(state="sleeping", poll_seconds=poll_seconds, next_scan_at=next_scan_at)

        print(f"[{ut.hoje_iso()} {ut.hora_agora()}] Varredura concluida")

        # dorme em "fatias" para responder rápido ao stop.flag
        step = 1  # 1s
        while True:
            if stop_requested():
                print(f"[{ut.hoje_iso()} {ut.hora_agora()}] Stop solicitado durante o sleep. Encerrando.")
                write_status(state="stopped")
                return

            remaining = next_scan_at - time.time()
            if remaining <= 0:
                break

            time.sleep(min(step, remaining))

if __name__ == "__main__":
    main()
