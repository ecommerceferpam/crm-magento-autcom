import os
from datetime import datetime, timedelta, timezone
from typing import Union, List, Dict, Any, Optional
import magento_client as mc
from dotenv import load_dotenv

load_dotenv()

# Configura integração Magento
mc.set_magento_config(
    domain=os.getenv("MAGENTO_DOMAIN"),
    token=os.getenv("MAGENTO_TOKEN")
)

UTC_MINUS_3 = timezone(timedelta(hours=-3))

def parse_utc_str(updated_at: str) -> datetime:
    """
    Converte "YYYY-MM-DD HH:MM:SS" (UTC) para datetime aware em UTC.
    """
    dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)

def fmt_utc3(dt_utc: datetime) -> str:
    """
    Recebe datetime aware em UTC e retorna string formatada em UTC-3.
    """
    return dt_utc.astimezone(UTC_MINUS_3).strftime("%Y-%m-%d %H:%M:%S")

def esta_abandonado(updated_at: str, now_utc: datetime, items: Union[List[Dict[str, Any]], Dict[str, Any]], email: Optional[str] = None, *, minutos: int = 10,) -> bool:
    """
    Retorna True se:
      - tem itens
      - tem email (recuperável)
      - updated_at (UTC) é mais antigo que agora (UTC) - minutos

    updated_at esperado: "YYYY-MM-DD HH:MM:SS" em UTC
    now_utc esperado: datetime aware em UTC
    """

    # precisa ter updated_at
    if not updated_at:
        return False

    # precisa ter now
    if not now_utc:
        return False

    # precisa ter email (recuperável)
    if not email or not isinstance(email, str) or "@" not in email:
        return False

    # normaliza items
    if isinstance(items, dict):
        items_list = items.get("items") or []
    elif isinstance(items, list):
        items_list = items
    else:
        items_list = []

    # precisa ter item
    if len(items_list) == 0:
        return False

    updated_dt_utc = parse_utc_str(updated_at)
    limite_utc = now_utc - timedelta(minutes=minutos)

    return updated_dt_utc < limite_utc

def listar_carrinhos_abandonados() -> Dict[str, Any]:
    try:
        # Tudo em UTC
        now_utc = datetime.now(timezone.utc)
        from_dt_utc = now_utc - timedelta(days=2)
        to_dt_utc = now_utc

        # Magento espera UTC naive ("YYYY-MM-DD HH:MM:SS" sem tzinfo)
        carrinhos = mc.listar_carrinhos_ativos(
            dt_from=from_dt_utc.replace(tzinfo=None),
            dt_to=to_dt_utc.replace(tzinfo=None),
        )

        abandonados = []

        for c in carrinhos:
            updated_at = c.get("updated_at")  # string UTC
            items = c.get("items")
            email = ((c.get("customer") or {}).get("email"))

            if esta_abandonado(
                updated_at=updated_at,
                now_utc=to_dt_utc,
                items=items,
                minutos=10,
                email=email):

                updated_dt_utc = parse_utc_str(updated_at)

                abandonados.append({
                    "cart_id": c.get("cart_id"),
                    "email": email,
                    "updated_at_utc": updated_at,
                    "updated_at_utc3": fmt_utc3(updated_dt_utc),
                    "valor_total": c.get("valor_total"),
                    "items": items,
                })

        return {
            "ok": True,
            "janela": {
                "from_utc": from_dt_utc.isoformat(),
                "to_utc": to_dt_utc.isoformat(),
                "from_utc3": fmt_utc3(from_dt_utc),
                "to_utc3": fmt_utc3(to_dt_utc),
            },
            "total_carrinhos": len(carrinhos),
            "total_abandonados": len(abandonados),
            "abandonados": abandonados,
        }

    except Exception as e:
        return {
            "ok": False,
            "erro": str(e),
        }
