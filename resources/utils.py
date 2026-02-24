from datetime import date, datetime
import re
from typing import Dict, Any, Optional
import autcom_connector as ac
from . import database as db
import os

ac.set_autcom_config(domain=os.getenv("AUTCOM_DOMAIN"), token=os.getenv("AUTCOM_TOKEN"))

# main.py -----------------------------------------------------------

def hoje_iso() -> str:
    return date.today().strftime("%Y-%m-%d")

def hora_agora() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _somente_digitos(valor: str) -> str:
    return re.sub(r"\D", "", str(valor or ""))

def e_cpf(valor: str) -> bool:
    cpf = _somente_digitos(valor)
    if len(cpf) != 11:
        return False
    if cpf == cpf[0] * 11:
        return False

    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    dig1 = (soma * 10) % 11
    dig1 = 0 if dig1 == 10 else dig1

    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    dig2 = (soma * 10) % 11
    dig2 = 0 if dig2 == 10 else dig2

    return cpf[-2:] == f"{dig1}{dig2}"

def e_cnpj(valor: str) -> bool:
    cnpj = _somente_digitos(valor)
    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False

    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    soma = sum(int(cnpj[i]) * pesos_1[i] for i in range(12))
    resto = soma % 11
    dig1 = 0 if resto < 2 else 11 - resto

    soma = sum(int(cnpj[i]) * pesos_2[i] for i in range(13))
    resto = soma % 11
    dig2 = 0 if resto < 2 else 11 - resto

    return cnpj[-2:] == f"{dig1}{dig2}"

# orders.py ---------------------------------------------------------
def abrir_crm(pedido: Dict[str, Any], codigo_cliente: str, cpf_cnpj: str, pedido_mag: str, data: str) -> Optional[Dict[str, Any]]:
    """
    Envia a ocorrência no CRM e, se der certo, registra no DB de envios.
    Se der erro, registra no DB de erros.
    """
    operador_origem_crm = "328" # DAYANE FRANÇA
    tipo_ocorrencia = "00043" # PROSPECÇÃO E-COMMERCER

    try:
        itens_adquiridos = []
        for i in pedido.get("items", []):
            sku = i.get("sku")
            name = i.get("name")
            qty = i.get("qty_ordered")
            itens_adquiridos.append(f"{qty}x {name} (SKU: {sku})")

        itens_adquiridos_txt = "; ".join(itens_adquiridos)

        descricao = (
            f"Este cliente efetuou o pedido id.: {pedido_mag} no site - "
            f"Aproveite para oferecer itens complementares e aumentar o valor da venda. "
            f"Itens já comprados: {itens_adquiridos_txt}"
        )

        ocorrencia = ac.criar_ocorrencia_atendimento_cliente(
            codigo_cliente=codigo_cliente,
            codigo_operador=operador_origem_crm,
            codigo_tipo_ocorrencia=tipo_ocorrencia,
            data_ocorrencia=data,
            hora_ocorrencia=hora_agora(),
            descricao_ocorrencia=descricao,
        )

        # Registra envio
        db.registrar_envio(cpf_cnpj=cpf_cnpj, codigo_cliente=codigo_cliente, data_hoje=data, pedido_mag=pedido_mag)
        print(ocorrencia)
        return ocorrencia

    except Exception as e:
        db.registrar_erro(
            origem="criar_ocorrencia",
            exc=e,
            cpf_cnpj=cpf_cnpj,
            codigo_cliente=codigo_cliente,
            pedido_mag=str(pedido_mag) if pedido_mag is not None else None,
        )
        print(f"Erro ao criar ocorrência: {e}")
        return None

# carts.py ----------------------------------------------------------
def abrir_crm_carrinho(items, valor, email: str, phone: Optional[str], data: str) -> Optional[Dict[str, Any]]:
    """
    Envia a ocorrência no CRM e, se der certo, registra no DB de envios.
    Se der erro, registra no DB de erros.
    """
    operador_origem_crm = "328" # DAYANE FRANÇA
    tipo_ocorrencia = "00044" # CARRINHO ABANDONADO
    cliente = "00221573" #cliente temporario para receber crms de carrinho abandonado, usei cnpj da LDM

    try:
        itens_no_carrinho = []
        for i in items:
            sku = i.get("sku")
            name = i.get("name")
            qty = i.get("qty")
            itens_no_carrinho.append(f"{qty}x {name} (SKU: {sku})")

        itens_no_carrinho_txt = "; ".join(itens_no_carrinho)

        descricao = (
            f"Cliente Email: {email} {f'Telefone: {phone} ' if phone else ''}abandonou um carrinho de R$ {valor} com os seguintes itens:"
            f"{itens_no_carrinho_txt}"
        )

        ocorrencia = ac.criar_ocorrencia_atendimento_cliente(
            codigo_cliente=cliente,
            codigo_operador=operador_origem_crm,
            codigo_tipo_ocorrencia=tipo_ocorrencia,
            data_ocorrencia=data,
            hora_ocorrencia=hora_agora(),
            descricao_ocorrencia=descricao,
        )

        # Registra envio
        db.registrar_envio(cpf_cnpj=None, codigo_cliente=email, data_hoje=data, pedido_mag=None)
        print(ocorrencia)
        return ocorrencia

    except Exception as e:
        db.registrar_erro(
            origem="criar_ocorrencia_carrinho",
            exc=e,
            cpf_cnpj=None,
            codigo_cliente=email,
            pedido_mag=None,
        )
        print(f"Erro ao criar ocorrência: {e}")
        return None
    