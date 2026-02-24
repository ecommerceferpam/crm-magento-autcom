import magento_client as mc
import autcom_connector as ac
from . import utils as ut, database as db
import re, os
import unicodedata
from dotenv import load_dotenv
from .utils import abrir_crm


load_dotenv()  

# Configura integração Magento
mc.set_magento_config(domain=os.getenv("MAGENTO_DOMAIN"), token=os.getenv("MAGENTO_TOKEN"))
# Configura integração Autcom
ac.set_autcom_config(domain=os.getenv("AUTCOM_DOMAIN"), token=os.getenv("AUTCOM_TOKEN"))

#Configs CRM
valor_min_pedido_pf = 2000.00

def normalizar(texto: str) -> str:
    if not texto:
        return ""
    # remove acentos
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto.lower()

def processar_varredura(janela_de_minutos: int, modules: dict | None = None) -> None:
    """
    Uma varredura: busca pedidos recentes, decide quem manda CRM, evita duplicar no dia, registra erros.
    Agora controlada por módulos/config vindos do Streamlit (control.json).
    """

    # ---- defaults (pra não quebrar chamadas antigas) ----
    modules = modules or {}

    cfg_pf = modules.get("pedidos_cpf", {})
    cfg_pj = modules.get("pedidos_cnpj", {})
    cfg_kw = modules.get("palavras_chave", {})


    pf_enabled = bool(cfg_pf.get("enabled", True))
    pj_enabled = bool(cfg_pj.get("enabled", True))
    kw_enabled = bool(cfg_kw.get("enabled", False))

    pf_min = float(cfg_pf.get("min_value", 0.0))
    pj_min = float(cfg_pj.get("min_value", 0.0))  # se quiser aplicar min também em PJ
    kw_min = float(cfg_kw.get("min_value", 0.0))
    keywords = [str(k).strip().lower() for k in (cfg_kw.get("keywords", []) or []) if str(k).strip()]

    janela_de_minutos = max(1, janela_de_minutos)

    # ---- 1) Pedidos recentes ----
    try:
        pedidos = mc.buscar_ultimos_pedidos(janela_de_minutos=janela_de_minutos)
    except Exception as e:
        db.registrar_erro(origem="buscar_ultimos_pedidos", exc=e)
        pedidos = []

    data_hoje = ut.hoje_iso()

    # ---- 2) Processa pedidos ----
    for x in pedidos:
        increment_id = x.get("increment_id")
        cpf_cnpj = re.sub(r"\D", "", str(x.get("customer_taxvat") or ""))
        total = float(x.get("base_grand_total") or 0)

        is_pf = ut.e_cpf(cpf_cnpj)
        is_pj = ut.e_cnpj(cpf_cnpj)

        # ---- 2.1) Regras de elegibilidade por módulo ----
        allow_pf = pf_enabled and is_pf and total >= pf_min
        allow_pj = pj_enabled and is_pj and total >= pj_min

        # "Pedidos com palavras-chave" (se o pedido contiver keywords em algum texto)
        allow_kw = False
        items = x.get("items") or []

        if kw_enabled and keywords and total >= kw_min:

            # 🔹 Pega todos os nomes dos itens
            nomes_itens = [
                str(item.get("name") or "")
                for item in items
            ]

            texto_pedido = normalizar(" ".join(nomes_itens))

            # 🔹 Normaliza keywords
            keywords_norm = [normalizar(k) for k in keywords if k]

            # 🔹 Match por palavra inteira (evita substring fraca)
            allow_kw = any(
                re.search(rf"\b{re.escape(k)}\b", texto_pedido)
                for k in keywords_norm
            )

        # Se nenhum módulo aprovou, pula
        if not (allow_pf or allow_pj or allow_kw):
            continue

        # ---- 2.2) Consulta no Autcom ----
        try:
            codigo_cliente = ac.client.get_client().get(f"/cliente/{cpf_cnpj}").get("codigoCliente")
        except Exception as e:
            db.registrar_erro(
                origem="autcom_get_cliente",
                exc=e,
                cpf_cnpj=cpf_cnpj,
                codigo_cliente=None,
                pedido_mag=str(increment_id) if increment_id is not None else None,
            )
            continue

        if not codigo_cliente:
            continue

        # ✅ Não manda mais de 1x por dia por cliente
        if db.cliente_ja_recebeu_hoje(codigo_cliente=codigo_cliente, data_hoje=data_hoje):
            continue

        abrir_crm(
            pedido=x,
            codigo_cliente=codigo_cliente,
            cpf_cnpj=cpf_cnpj,
            pedido_mag=increment_id,
            data=data_hoje,
        )
