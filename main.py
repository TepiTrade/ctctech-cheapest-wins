# -*- coding: utf-8 -*-
import os, unicodedata
import pandas as pd
from core.select import pick_winners  # não importa nada de main.py

# fontes.txt aceito nestes locais
FONTE_CANDIDATOS = ["dados/fontes.txt", "data/dados/fontes.txt"]
# pastas locais aceitas como fallback (CSV no repo)
PASTAS_CSV = ["dados/feeds_de_amostra", "data/dados/feeds_de_amostra"]

def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _col(df: pd.DataFrame, candidatos):
    cols_lower = {str(c).lower(): c for c in df.columns}
    for nome in candidatos:
        k = str(nome).lower()
        if k in cols_lower:
            return cols_lower[k]
    return None

def _ler_fontes_urls() -> list[str]:
    for caminho in FONTE_CANDIDATOS:
        if os.path.exists(caminho):
            with open(caminho, "r", encoding="utf-8") as f:
                urls = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            if urls:
                print(f"Fontes (URLs): {len(urls)} de {caminho}")
                return urls
    return []

def _carregar_de_urls(urls: list[str]) -> pd.DataFrame:
    dfs = []
    for u in urls:
        try:
            df = pd.read_csv(u, dtype=str, encoding="utf-8")
            df["fonte"] = u
            dfs.append(df)
        except Exception as e:
            print(f"Falha ao ler URL: {u} -> {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def _carregar_de_pastas() -> pd.DataFrame:
    from glob import glob
    dfs = []
    for pasta in PASTAS_CSV:
        if os.path.isdir(pasta):
            for arq in sorted(glob(os.path.join(pasta, "*.csv"))):
                try:
                    df = pd.read_csv(arq, dtype=str, encoding="utf-8")
                    df["fonte"] = arq
                    dfs.append(df)
                except Exception as e:
                    print(f"Falha ao ler arquivo: {arq} -> {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def _padronizar(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    col_titulo = _col(df, ["titulo", "título", "title", "produto", "nome"])
    if col_titulo is None:
        df = df.copy()
        df["titulo"] = df.astype(str).apply(lambda r: r.iloc[0], axis=1)
        col_titulo = "titulo"

    col_preco = _col(df, ["preço", "preco", "price", "valor"])
    if col_preco is None:
        df = df.copy()
        df["preço"] = pd.NA
        col_preco = "preço"

    out = df.copy()
    out.rename(columns={col_titulo: "titulo_std", col_preco: "preco_std"}, inplace=True)

    # vírgula decimal -> ponto
    out["preco_std"] = out["preco_std"].astype(str).str.replace(",", ".", regex=False)
    out["preco_std"] = pd.to_numeric(out["preco_std"], errors="coerce")

    out["titulo_norm"] = out["titulo_std"].map(_norm)
    return out

def principal():
    # 1) tenta URLs do fontes.txt
    urls = _ler_fontes_urls()
    if urls:
        bruto = _carregar_de_urls(urls)
    else:
        print("Aviso: fontes.txt não encontrado ou vazio. Usando pastas locais.")
        bruto = _carregar_de_pastas()

    base = _padronizar(bruto)
    if base.empty:
        print("Sem dados válidos.")
        return

    groups = base.groupby("titulo_norm")
    winners = pick_winners(groups, cfg={})

    saida = "vencedores.csv"
    winners.to_csv(saida, index=False)

    print(f"Itens totais: {len(base)} | Grupos: {base['titulo_norm'].nunique()} | Vencedores: {len(winners)}")
    print(f"Arquivo gerado: {saida}")

if __name__ == "__main__":
    principal()
