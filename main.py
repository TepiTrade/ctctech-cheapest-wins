import os
import csv
import glob
import logging
from typing import List, Dict
import io

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Pasta onde ficam fontes.txt e os CSVs locais
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "dados", "dados")


def get_config() -> Dict[str, str]:
    """Lê variáveis de ambiente vindas dos secrets do GitHub."""
    base_url = os.environ.get("WC_BASE_URL", "").rstrip("/")
    ck = os.environ.get("WC_CK", "")
    cs = os.environ.get("WC_CS", "")

    if not base_url or not ck or not cs:
        raise RuntimeError(
            "WC_BASE_URL, WC_CK ou WC_CS não configurados nos secrets."
        )

    button_text = os.environ.get("DEFAULT_BUTTON_TEXT", "Comprar agora")
    currency = os.environ.get("DEFAULT_CURRENCY", "BRL")

    return {
        "base_url": base_url,
        "ck": ck,
        "cs": cs,
        "button_text": button_text,
        "currency": currency,
    }


def get_source_urls() -> List[str]:
    """Lê a lista de URLs de feeds em dados/dados/fontes.txt, se existir."""
    path = os.path.join(DATA_DIR, "fontes.txt")
    urls: List[str] = []

    if not os.path.exists(path):
        logging.warning("Arquivo fontes.txt não encontrado em %s", path)
        return urls

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)

    logging.info("Encontradas %d fontes em fontes.txt", len(urls))
    return urls


def find_csv_files() -> List[str]:
    """Procura arquivos CSV locais na pasta dados/dados/."""
    pattern = os.path.join(DATA_DIR, "*.csv")
    files = sorted(glob.glob(pattern))
    return files


def parse_product_row(row: Dict[str, str], cfg: Dict[str, str]) -> Dict:
    """Converte uma linha do CSV em um objeto de produto WooCommerce."""
    # Normaliza chaves para minúsculas
    normalized = {k.lower().strip(): (v or "").strip() for k, v in row.items()}

    name = normalized.get("name") or normalized.get("nome")
    regular_price = normalized.get("regular price") or normalized.get("regular_price")
    sku = normalized.get("sku")

    if not name or not regular_price:
        # Linha inválida para criação de produto
        return {}

    sale_price = normalized.get("sale price") or normalized.get("sale_price") or ""
    description = (
        normalized.get("description")
        or normalized.get("descricao")
        or ""
    )
    short_description = (
        normalized.get("short description")
        or normalized.get("short_description")
        or ""
    )

    # Categorias
    categories_raw = (
        normalized.get("categories")
        or normalized.get("categoria")
        or normalized.get("categorias")
        or ""
    )
    categories = []
    if categories_raw:
        for part in categories_raw.replace("|", ",").split(","):
            name_cat = part.strip()
            if name_cat:
                categories.append({"name": name_cat})

    # Tags
    tags_raw = normalized.get("tags") or ""
    tags = []
    if tags_raw:
        for part in tags_raw.replace("|", ",").split(","):
            tag_name = part.strip()
            if tag_name:
                tags.append({"name": tag_name})

    # Imagens
    images_raw = (
        normalized.get("images")
        or normalized.get("imagens")
        or normalized.get("image")
        or ""
    )
    images = []
    if images_raw:
        for url in images_raw.split(","):
            url = url.strip()
            if url:
                images.append({"src": url})

    product_type = (
        normalized.get("type")
        or normalized.get("tipo")
        or "simple"
    )

    meta_data = []

    # Guarda texto padrão do botão, se quiser usar depois
    if cfg.get("button_text"):
        meta_data.append(
            {"key": "_ctctech_button_text", "value": cfg["button_text"]}
        )

    if cfg.get("currency"):
        meta_data.append(
            {"key": "_ctctech_currency", "value": cfg["currency"]}
        )

    product = {
        "name": name,
        "type": product_type,
        "regular_price": str(regular_price),
        "sku": sku or "",
        "description": description,
        "short_description": short_description,
        "categories": categories,
        "tags": tags,
        "images": images,
        "meta_data": meta_data,
    }

    if sale_price:
        product["sale_price"] = str(sale_price)

    return product


def load_products_from_csv(path: str, cfg: Dict[str, str]) -> List[Dict]:
    logging.info(f"Lendo CSV local: {path}")
    products: List[Dict] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p = parse_product_row(row, cfg)
            if p:
                products.append(p)

    logging.info(f"Encontrados {len(products)} produtos válidos em {path}")
    return products


def load_products_from_remote_csv(url: str, cfg: Dict[str, str]) -> List[Dict]:
    logging.info(f"Baixando feed remoto: {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    products: List[Dict] = []
    f = io.StringIO(resp.text)
    reader = csv.DictReader(f)

    for row in reader:
        p = parse_product_row(row, cfg)
        if p:
            products.append(p)

    logging.info(f"Encontrados {len(products)} produtos válidos em {url}")
    return products


def send_to_woocommerce(products: List[Dict], cfg: Dict[str, str]) -> None:
    """Envia produtos para o WooCommerce via /products/batch."""
    if not products:
        logging.info("Nenhum produto para enviar ao WooCommerce.")
        return

    base_url = cfg["base_url"]
    ck = cfg["ck"]
    cs = cfg["cs"]

    url = f"{base_url}/wp-json/wc/v3/products/batch"

    # WooCommerce aceita batch de até ~100 por vez com segurança
    batch_size = 50
    total = len(products)
    sent = 0

    for i in range(0, total, batch_size):
        chunk = products[i : i + batch_size]
        payload = {"create": chunk}

        logging.info(
            f"Enviando lote {i // batch_size + 1} com {len(chunk)} produtos..."
        )

        resp = requests.post(
            url,
            auth=(ck, cs),
            json=payload,
            timeout=120,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logging.error("Erro ao enviar lote para WooCommerce: %s", e)
            logging.error("Resposta: %s", resp.text)
            raise

        data = resp.json()
        created = data.get("create", [])
        sent += len(created)
        logging.info(f"Lote enviado com sucesso. Criados {len(created)} produtos.")

    logging.info(f"Envio concluído. Total de produtos criados: {sent}.")


def main() -> None:
    logging.info("Iniciando migração automática de produtos afiliados...")

    cfg = get_config()
    csv_files = find_csv_files()
    source_urls = get_source_urls()

    if not csv_files and not source_urls:
        print("Sem dados nos CSVs.")
        logging.warning(
            "Nenhum arquivo CSV local em %s e nenhuma URL em fontes.txt.",
            DATA_DIR,
        )
        return

    all_products: List[Dict] = []

    # CSVs locais (ex.: shein.csv)
    for path in csv_files:
        prods = load_products_from_csv(path, cfg)
        all_products.extend(prods)

    # Feeds remotos (Amazon, Shopee, Mercado Livre, AliExpress etc.)
    for url in source_urls:
        prods = load_products_from_remote_csv(url, cfg)
        all_products.extend(prods)

    if not all_products:
        print("Sem produtos válidos encontrados nos feeds.")
        logging.warning("Feeds processados, mas sem produtos válidos.")
        return

    send_to_woocommerce(all_products, cfg)
    logging.info("Migração concluída com sucesso.")


if __name__ == "__main__":
    main()
