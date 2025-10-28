import requests, json

class WooClient:
    def __init__(self, base_url, ck, cs, default_currency, button_text):
        self.base = base_url.rstrip("/")
        self.ck = ck
        self.cs = cs
        self.btn = button_text
        self.currency = default_currency

    def _req(self, method, path, **kw):
        url = f"{self.base}/wp-json/wc/v3{path}"
        auth = (self.ck, self.cs)
        r = requests.request(method, url, auth=auth, timeout=60, **kw)
        if not r.ok:
            raise RuntimeError(f"{method} {path} {r.status_code}: {r.text[:200]}")
        return r.json()

    def find_product_by_sku(self, sku: str):
        js = self._req("GET", "/products", params={"sku": sku})
        return js[0] if js else None

    def create_product(self, p: dict):
        return self._req("POST", "/products", json=p)

    def update_product(self, pid: int, p: dict):
        return self._req("PUT", f"/products/{pid}", json=p)

def to_woo_payload(row, cfg):
    category = row.get("category") or cfg.get("category_fallback", "Importados")
    cats = [{"name": category}]
    payload = {
        "name": row.get("title"),
        "type": "external",
        "sku": row.get("sku"),
        "regular_price": str(max(0.0, float(row.get('price', 0.0)))),
        "external_url": row.get("url"),
        "button_text": row.get("button_text") or "Comprar",
        "images": [{"src": row.get("image")}] if row.get("image") else [],
        "categories": cats,
        "meta_data": [
            {"key":"platform","value":row.get("platform")},
            {"key":"total_brl","value":row.get("total_brl",0.0)},
            {"key":"currency","value":row.get("currency")},
        ],
        "catalog_visibility": "visible",
        "status": "publish",
    }
    return payload

def upsert_products(wc: WooClient, winners_df, cfg):
    created, updated = 0, 0
    for _, row in winners_df.iterrows():
        sku = row.get("sku")
        existing = wc.find_product_by_sku(sku)
        payload = to_woo_payload(row, cfg)
        payload["button_text"] = wc.btn
        if existing:
            wc.update_product(existing["id"], payload)
            updated += 1
        else:
            wc.create_product(payload)
            created += 1
    print(f"Produtos criados: {created} | atualizados: {updated}")
