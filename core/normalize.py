import re, hashlib
import pandas as pd
from slugify import slugify

STOPWORDS = {"de","da","do","para","e","com","sem","the","a","an","and"}

def clean_text(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def guess_model(title: str) -> str:
    # heurística simples: pega tokens tipo "A12", "M2", "X-100", "GTX1050"
    t = clean_text(title)
    m = re.findall(r"\b([a-z0-9]+[- ]?[a-z0-9]{2,})\b", t)
    return m[0] if m else ""

def canonical_key(row) -> str:
    brand = clean_text(row.get("brand",""))
    model = clean_text(row.get("model","")) or guess_model(row.get("title",""))
    title = clean_text(row.get("title",""))
    title_tokens = [w for w in re.split(r"[^a-z0-9]+", title) if w and w not in STOPWORDS]
    title_slug = "-".join(title_tokens[:8])
    return f"{brand}|{model}|{title_slug}"

def norm_brand(s: str) -> str:
    s = clean_text(s)
    s = re.sub(r"[^a-z0-9]+","",s)
    return s

def normalize_record_df(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    df["brand_norm"] = df["brand"].map(norm_brand)
    df["model_norm"] = df["model"].map(clean_text)
    df["title_norm"] = df["title"].map(clean_text)
    df["key"] = df.apply(canonical_key, axis=1)
    # ID determinístico para SKU
    df["sku_hash"] = df["key"].map(lambda x: hashlib.sha256(x.encode()).hexdigest()[:8])
    df["sku"] = df["brand_norm"].fillna("") + "-" + df["model_norm"].fillna("") + "-" + df["sku_hash"]
    return df
