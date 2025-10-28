import pandas as pd

REQUIRED_COLS = ["title","brand","model","price","currency","shipping","fee_percent","url","image","category","platform"]

def _read_one(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path, sep=";")
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    for c in missing:
        df[c] = "" if c not in ["price","fee_percent"] else 0.0
    # tipagem básica
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["fee_percent"] = pd.to_numeric(df["fee_percent"], errors="coerce").fillna(0.0)
    df["shipping"] = pd.to_numeric(df["shipping"], errors="coerce").fillna(0.0)
    return df[REQUIRED_COLS]

def load_sources(sources):
    frames = []
    for src in sources:
        if src.startswith("http://") or src.startswith("https://"):
            # Em nuvem, GitHub Actions pode baixar via requests + BytesIO, mas simplificamos aqui
            # para o exemplo local: usuário pode baixar antes.
            try:
                import requests, io
                r = requests.get(src, timeout=30)
                r.raise_for_status()
                df = pd.read_csv(io.StringIO(r.text))
            except Exception:
                continue
        else:
            df = _read_one(src)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=REQUIRED_COLS)
    return pd.concat(frames, ignore_index=True)
