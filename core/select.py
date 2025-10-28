# -*- coding: utf-8 -*-
import pandas as pd

def _col(gdf: pd.DataFrame, candidatos):
    """Retorna o NOME exato da coluna existente no DF, dado uma lista de candidatos em minúsculas."""
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        if nome in cols_lower:
            return cols_lower[nome]
    return None

def pick_winners(groups, cfg):
    """Escolhe o item mais barato dentro de cada grupo. Tolera cabeçalhos em PT/EN: preço/preco/price/valor."""
    vencedores = []

    for _, gdf in groups:
        # 1) Detecta a coluna de preço com tolerância a variações
        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])

        # 2) Se veio algo com .name (Série, Index), extraia o NOME
        if hasattr(col_preco, "name"):
            col_preco = col_preco.name

        # 3) Se não for uma coluna válida, tenta a primeira coluna numérica
        if col_preco is None or col_preco not in gdf.columns:
            num_cols = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = num_cols[0] if num_cols else None

        # 4) Se ainda não há coluna válida, pega a primeira linha do grupo
        if col_preco is None:
            vencedores.append(gdf.iloc[0])
            continue

        # 5) Converte para numérico para ordenar corretamente
        gdf[col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')

        # 6) Se tudo virou NaN, retorna a primeira linha
        if gdf[col_preco].notna().sum() == 0:
            vencedores.append(gdf.iloc[0])
            continue

        # 7) Ordena pelo NOME da coluna (string), nunca por Série
        gdf_sorted = gdf.sort_values(by=[col_preco], ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
