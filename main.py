import os
import csv
import logging
from typing import List, Dict, Optional
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# URLs dos feeds CSV – depois você troca pelos feeds reais (Magalu, Amazon, Shopee etc.)
FEED_URLS: List[str] = [
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/amazon.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/aliexpress.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/mercadolivre.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/ela.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/shopee.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/feeds_de_amostra/temu.csv",
    "https://raw.githubusercontent.com/TepiTrade/ctctech-cheapest-wins/main/dados/dados/shein.csv",
]


def get_config() -> Dict[str, str]:
    """Lê as variáveis de ambiente vindas dos segredos do GitHub."""
    base_url = os.environ.get("WC_BASE_URL", "").rstrip("/")
    ck = os.environ.get("WC_CK", "")
    cs = os.environ.get("WC_CS", "")
    default_button_text = os.environ.get("DEFAULT_BUTTON_TEXT", "Comprar agora")
    default_currency = os.environ.get("DEFAULT_CURRENCY", "BRL")

    if not base_url or not ck or not cs:
        raise RuntimeError("WC_BASE_URL, WC_CK ou WC_CS não configurados nos segredos.")

    return {
        "base_url": base_url,
        "ck": ck,
        "cs": cs,
        "button_text": default_button_text,
        "currency": default_currency,
    }


def _first_non_empty(row: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None:
            v = str(v).strip()
            if v:
                return v
    return ""


def parse_product_row(row: Dict[str, str], cfg: Dict[str, str]) -> Optional[Dict]:
    """Converte uma linha do CSV em produto WooCommerce."""
    name = _first_non_empty(row, ["Name", "nome", "title", "Título"])
    sku = _first_non_empty(row, ["SKU", "sku", "codigo", "código"])

    regular_price_str = _first_non_empty(
        row,
        ["Regular price", "regular_price", "preco_normal", "preço_normal", "price"],
    )
    sale_price_str = _first_non_empty(
        row,
        ["Sale price", "sale_price", "preco_de_venda", "preço_de_venda", "promo_price"],
    )

    if not name or not regular_price_str:
        return None

    def _to_float(s: str) -> float:
        s = s.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return 0.0

    regular_price = _to_float(regular_price_str)
    sale_price = _to_float(sale_price_str) if sale_price_str else 0.0

    description = _first_non_empty(
        row, ["Description", "descricao", "descrição", "Long description"]
    )
    short_description = _first_non_empty(
        row, ["Short description", "descricao_curta", "descrição_curta"]
    )

    categories_raw = _first_non_empty(row, ["Categories", "categorias", "categoria"])
    categories: List[Dict[str, str]] = []
    if categories_raw:
        for part in categories_raw.split(","):
            part = part.strip()
            if part:
                categories.append({"name": part})

    tags_raw = _first_non_empty(row, ["Tags", "etiquetas", "tags"])
    tags: List[Dict[str, str]] = []
    if tags_raw:
        for part in tags_raw.split(","):
            part = part.strip()
            if part:
                tags.append({"name": part})

    images_raw = _first_non_empty(row, ["Images", "images", "imagens"])
    images: List[Dict[str, str]] = []
    if images_raw:
        sep = "|" if "|" in images_raw else ","
        for url in images_raw.split(sep):
            url = url.strip()
            if url:
                images.append({"src": url})

    meta_data = [
        {"key": "_ctctech_button_text", "value": cfg["button_text"]},
        {"key": "_ctctech_currency", "value": cfg["currency"]},
    ]

    product: Dict[str, object] = {
        "name": name,
        "type": "simple",
        "regular_price": f"{regular_price:.2f}",
        "description": description,
        "short_description": short_description,
        "sku": sku or "",
        "categories": categories,
        "tags": tags,
        "images": images,
        "meta_data": meta_data,
    }

    if sale_price > 0 and sale_price < regular_price:
        product["sale_price"] = f"{sale_price:.2f}"

    return product


def download_feed(url: str, cfg: Dict[str, str]) -> List[Dict]:
    logging.info("Baixando feed CSV: %s", url)
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except Exception as e:
        logging.error("Erro ao baixar feed %s: %s", url, e)
        return []

    lines = resp.text.splitlines()
    if not lines:
        logging.warning("Feed vazio: %s", url)
        return []

    reader = csv.DictReader(lines)
    products: List[Dict] = []
    for row in reader:
        p = parse_product_row(row, cfg)
        if p:
            products.append(p)

    logging.info("Feed %s gerou %d produtos válidos.", url, len(products))
    return products


def dedupe_cheapest(products: List[Dict]) -> List[Dict]:
    """Mantém apenas o produto mais barato por SKU."""
    best_by_sku: Dict[str, Dict] = {}

    def price_of(p: Dict) -> float:
        sale = p.get("sale_price")
        regular = p.get("regular_price")

        def _to_f(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return 0.0

        if sale:
            return _to_f(sale)
        return _to_f(regular)

    for p in products:
        sku = (p.get("sku") or "").strip()
        key = sku or f"__no_sku__{p.get('name','')}"
        if key not in best_by_sku:
            best_by_sku[key] = p
        else:
            if price_of(p) < price_of(best_by_sku[key]):
                best_by_sku[key] = p

    result = list(best_by_sku.values())
    logging.info("Após deduplicação por preço/sku, restaram %d produtos.", len(result))
    return result


def send_to_woocommerce(products: List[Dict], cfg: Dict[str, str]) -> None:
    if not products:
        logging.warning("Nenhum produto para enviar ao WooCommerce.")
        return

    base_url = cfg["base_url"]
    ck = cfg["ck"]
    cs = cfg["cs"]

    url = f"{base_url}/wp-json/wc/v3/products/batch"

    batch_size = 50
    total = len(products)
    sent = 0

    for i in range(0, total, batch_size):
        batch = products[i : i + batch_size]
        payload = {"create": batch}
        logging.info(
            "Enviando lote %d/%d com %d produtos...",
            i // batch_size + 1,
            (total + batch_size - 1) // batch_size,
            len(batch),
        )
        try:
            resp = requests.post(
                url,
                auth=(ck, cs),
                json=payload,
                timeout=300,
            )
            resp.raise_for_status()
        except Exception as e:
            logging.error("Erro ao enviar lote para WooCommerce: %s", e)
            if hasattr(e, "response") and getattr(e, "response") is not None:
                logging.error("Resposta: %s", e.response.text)
            raise

        data = resp.json()
        created = data.get("create", [])
        sent += len(created)
        logging.info("Lote enviado com sucesso. Criados %d produtos.", len(created))

    logging.info("Envio concluído. Total de produtos criados: %d", sent)


def main() -> None:
    logging.info("Iniciando migração automática de produtos afiliados...")

    cfg = get_config()

    all_products: List[Dict] = []
    for url in FEED_URLS:
        prods = download_feed(url, cfg)
        all_products.extend(prods)

    if not all_products:
        logging.warning("Nenhum produto válido encontrado em nenhum feed.")
        print("Sem dados nos feeds CSV.")
        return

    final_products = dedupe_cheapest(all_products)
    send_to_woocommerce(final_products, cfg)
    logging.info("Migração concluída com sucesso.")


if __name__ == "__main__":
    main()
