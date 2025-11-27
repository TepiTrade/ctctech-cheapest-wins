import os
import csv
import glob
import logging
from typing import List, Dict

import requests

# Log básico
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Diretórios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "dados", "dados")

# Limites de segurança
MAX_PRODUTOS_POR_EXECUCAO = int(os.getenv("MAX_PRODUTOS_POR_EXECUCAO", "200"))
MAX_PRODUTOS_POR_FONTE = int(os.getenv("MAX_PRODUTOS_POR_FONTE", "200"))
TAMANHO_LOTE_WC = 50  # produtos por lote no WooCommerce


def get_config() -> Dict[str, str]:
    """
    Lê configuração a partir dos segredos do GitHub (variáveis de ambiente).
    """
    base_url = os.getenv("WC_BASE_URL", "").rstrip("/")
    ck = os.getenv("WC_CK", "")
    cs = os.getenv("WC_CS", "")
    button_text = os.getenv("DEFAULT_BUTTON_TEXT", "")
    currency = os.getenv("DEFAULT_CURRENCY", "BRL")

    if not base_url or not ck or not cs:
        raise RuntimeError(
            "WC_BASE_URL, WC_CK ou WC_CS não configurados nos segredos."
        )

    return {
        "base_url": base_url,
        "ck": ck,
        "cs": cs,
        "button_text": button_text,
        "currency": currency,
    }


def get_local_csv_files() -> List[str]:
    """
    Retorna todos os arquivos CSV locais em dados/dados/*.csv
    (por exemplo, dados/dados/shein.csv, dados/dados/amazon.csv, etc.)
    """
    pattern = os.path.join(DATA_DIR, "*.csv")
    files = sorted(glob.glob(pattern))
    logging.info("Encontrados %s arquivos CSV locais em %s", len(files), DATA_DIR)
    return files


def get_feed_urls() -> List[str]:
    """
    Lê URLs de feeds CSV a partir de dados/dados/fontes.txt (uma por linha).
    Se o arquivo não existir, simplesmente não usa feeds remotos.
    """
    path = os.path.join(DATA_DIR, "fontes.txt")
    if not os.path.exists(path):
        logging.warning("Arquivo fontes.txt não encontrado em %s", path)
        return []

    urls: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)

    logging.info("Encontradas %s URLs em fontes.txt", len(urls))
    return urls


def _normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Normaliza chaves e valores do CSV para facilitar o mapeamento.
    """
    norm: Dict[str, str] = {}
    for k, v in row.items():
        key = (k or "").strip().lower()
        val = (v or "").strip()
        norm[key] = val
    return norm


def _split_list(text: str) -> List[str]:
    if not text:
        return []
    return [p.strip() for p in text.split(",") if p.strip()]


def row_to_product(row: Dict[str, str], cfg: Dict[str, str]) -> Dict:
    """
    Converte uma linha do CSV em objeto de produto WooCommerce.
    Aceita cabeçalhos em português ou inglês.
    Campos esperados no CSV (nomes possíveis):
      - name / nome
      - regular_price / preço_normal / preco_normal
      - sale_price / preço_de_venda / preco_de_venda (opcional)
      - sku
      - description / descrição / descricao
      - short_description / descrição_curta / descricao_curta
      - categories / categorias  (separadas por vírgula)
      - tags / etiquetas          (separadas por vírgula)
      - images / imagens          (URLs separadas por | )
      - type / tipo               (simple, external, etc; default simple)
      - affiliate_url / link      (URL de afiliado completa)
    """
    norm = _normalize_row(row)

    name = norm.get("name") or norm.get("nome")
    regular_price = (
        norm.get("regular_price")
        or norm.get("preço_normal")
        or norm.get("preco_normal")
    )
    sale_price = (
        norm.get("sale_price")
        or norm.get("preço_de_venda")
        or norm.get("preco_de_venda")
        or ""
    )
    sku = norm.get("sku", "")

    description = (
        norm.get("description")
        or norm.get("descrição")
        or norm.get("descricao")
        or ""
    )
    short_description = (
        norm.get("short_description")
        or norm.get("descrição_curta")
        or norm.get("descricao_curta")
        or ""
    )

    categories_raw = norm.get("categories") or norm.get("categorias") or ""
    tags_raw = norm.get("tags") or norm.get("etiquetas") or ""
    images_raw = norm.get("images") or norm.get("imagens") or ""
    product_type = norm.get("type") or norm.get("tipo") or "simple"
    affiliate_url = norm.get("affiliate_url") or norm.get("link") or ""

    if not name or not regular_price:
        # Linha inválida para criação de produto
        return {}

    categories = [{"name": c} for c in _split_list(categories_raw)]
    tags = [{"name": t} for t in _split_list(tags_raw)]

    images: List[Dict[str, str]] = []
    if images_raw:
        for url in images_raw.split("|"):
            u = url.strip()
            if u:
                images.append({"src": u})

    meta_data: List[Dict[str, str]] = []

    if cfg.get("button_text"):
        meta_data.append(
            {"key": "_ctctech_button_text", "value": cfg["button_text"]}
        )
    if cfg.get("currency"):
        meta_data.append({"key": "_ctctech_currency", "value": cfg["currency"]})
    if affiliate_url:
        # Meta para armazenar o link de afiliado original
        meta_data.append({"key": "_ctctech_affiliate_url", "value": affiliate_url})

    product: Dict = {
        "name": name,
        "type": product_type,
        "regular_price": str(regular_price),
        "description": description,
        "short_description": short_description,
        "sku": sku,
        "categories": categories,
        "tags": tags,
        "images": images,
        "meta_data": meta_data,
    }

    if sale_price:
        product["sale_price"] = str(sale_price)

    return product


def _load_from_dict_reader(
    reader: csv.DictReader, cfg: Dict[str, str], max_items: int
) -> List[Dict]:
    produtos: List[Dict] = []
    for row in reader:
        if len(produtos) >= max_items:
            break
        p = row_to_product(row, cfg)
        if p:
            produtos.append(p)
    return produtos


def load_products_from_local_csv(path: str, cfg: Dict[str, str]) -> List[Dict]:
    logging.info("Lendo CSV local: %s", path)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return _load_from_dict_reader(reader, cfg, MAX_PRODUTOS_POR_FONTE)


def load_products_from_url_csv(url: str, cfg: Dict[str, str]) -> List[Dict]:
    logging.info("Baixando CSV remoto: %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    return _load_from_dict_reader(reader, cfg, MAX_PRODUTOS_POR_FONTE)


def send_to_woocommerce(produtos: List[Dict], cfg: Dict[str, str]) -> None:
    if not produtos:
        logging.info("Nenhum produto para enviar ao WooCommerce.")
        return

    base_url = cfg["base_url"]
    ck = cfg["ck"]
    cs = cfg["cs"]

    url = f"{base_url}/wp-json/wc/v3/products/batch"

    total = len(produtos)
    enviado = 0

    for i in range(0, total, TAMANHO_LOTE_WC):
        lote = produtos[i : i + TAMANHO_LOTE_WC]
        payload = {"create": lote}

        logging.info(
            "Enviando lote %s (%s produtos)...", (i // TAMANHO_LOTE_WC) + 1, len(lote)
        )

        resp = requests.post(url, auth=(ck, cs), json=payload, timeout=120)

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logging.error("Erro ao enviar lote para WooCommerce: %s", e)
            logging.error("Resposta: %s", resp.text)
            raise

        data = resp.json()
        created = data.get("create", [])
        enviado += len(created)

        logging.info(
            "Lote enviado com sucesso. Criados: %s produtos.", len(created)
        )

    logging.info("Envio concluído. Total de produtos criados: %s", enviado)


def main() -> None:
    logging.info("Iniciando migração automática de produtos afiliados...")

    cfg = get_config()

    todos_os_produtos: List[Dict] = []

    # 1) CSVs locais em dados/dados/*.csv
    for path in get_local_csv_files():
        if len(todos_os_produtos) >= MAX_PRODUTOS_POR_EXECUCAO:
            logging.info(
                "Atingido limite MAX_PRODUTOS_POR_EXECUCAO (%s).",
                MAX_PRODUTOS_POR_EXECUCAO,
            )
            break

        prods = load_products_from_local_csv(path, cfg)
        for p in prods:
            if len(todos_os_produtos) >= MAX_PRODUTOS_POR_EXECUCAO:
                break
            todos_os_produtos.append(p)

    # 2) CSVs remotos (URLs em fontes.txt)
    for url in get_feed_urls():
        if len(todos_os_produtos) >= MAX_PRODUTOS_POR_EXECUCAO:
            logging.info(
                "Atingido limite MAX_PRODUTOS_POR_EXECUCAO (%s).",
                MAX_PRODUTOS_POR_EXECUCAO,
            )
            break

        prods = load_products_from_url_csv(url, cfg)
        for p in prods:
            if len(todos_os_produtos) >= MAX_PRODUTOS_POR_EXECUCAO:
                break
            todos_os_produtos.append(p)

    if not todos_os_produtos:
        logging.warning("Nenhum produto válido encontrado em nenhum CSV.")
        print("Sem dados nos CSVs.")
        return

    send_to_woocommerce(todos_os_produtos, cfg)
    logging.info("Migração concluída com sucesso.")


if __name__ == "__main__":
    main()
