# -*- coding: utf-8 -*-
import os, glob, unicodedata
import pandas as pd
from core.select import pick_winners  # sem importar _col para evitar ciclo

PASTA = "dados/feeds_de_amostra"

def _norm(s):
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _col(df: pd.DataFrame, candidatos):
    cols_lower = {str(c).lower(): c for c in df.columns}
    for nome in candidatos:
        n = str(nome).lower()
        if n in cols_lower:
            return cols_lower[n]
    return None

def _carregar_csvs(pasta):
    arquivos = sorted(glob.glob(os.path.join(pasta, "*.csv")))
    if not arquivos:
        return pd.DataFrame()
    dfs = []
    for arq in arquivos:
        try:
            df = pd.read_csv(arq)
            df["fonte"] = os.path.basename(arq)
            dfs.append(df)
        except Exception:
            continue
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def _padronizar(df):
    if df.empty:
        return df
    # título
    col_titulo = _col(df, ["titulo", "título", "title", "produto", "nome"])
    if col_titulo is None:
        df["titulo"] = df.apply(lambda r: r.astype(str).head(1).values[0], axis=1)
        col_titulo = "titulo"
    # preço
    col_preco = _col(df, ["preço", "preco", "price", "valor"])
    if col_preco is None:
        df["preço"] = pd.NA
        col_preco = "preço"
    out = df.copy()
    out.rename(columns={col_titulo: "titulo_std", col_preco: "preco_std"}, inplace=True)
    out["titulo_norm"] = out["titulo_std"].map(_norm)
    out["preco_std"] = pd.to_numeric(out["preco_std"], errors="coerce")
    return out

def principal():
    df = _carregar_csvs(PASTA)
    df = _padronizar(df)
    if df.empty:
        print("Sem dados nos CSVs.")
        return
    groups = df.groupby("titulo_norm")
    winners = pick_winners(groups, cfg={})
    saida = "vencedores.csv"
    winners.to_csv(saida, index=False)
    print(f"Vencedores: {len(winners)} linhas -> {saida}")

if __name__ == "__main__":
    principal()
