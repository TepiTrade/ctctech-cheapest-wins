# -*- coding: utf-8 -*-
import pandas as pd

def _col(gdf, candidatos):
    """Devolve o nome exato da coluna se existir, dado uma lista em minúsculas."""
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        if nome in cols_lower:
            return cols_lower[nome]
    return None

def pick_winners(groups, cfg):
    """Escolhe o item mais barato em cada grupo, tolerando cabeçalhos PT/EN."""
    vencedores = []

    for _, gdf in groups:
        # tenta mapear a coluna de preço por nomes comuns
        cand_nomes = ['preço', 'preco', 'price', 'valor']
        col_preco = _col(gdf, cand_nomes)

        # se veio um objeto com atributo .name (ex.: Série), use apenas o nome
        if hasattr(col_preco, "name"):
            col_preco = col_preco.name

        # fallback: primeira coluna numérica
        if col_preco is None or col_preco not in gdf.columns:
            num_cols = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = num_cols[0] if num_cols else None

        # se ainda não houver coluna válida, pega a primeira linha do grupo
        if col_preco is None:
            vencedores.append(gdf.iloc[0])
            continue

        # força numérico para ordenar corretamente
        gdf[col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')

        if gdf[col_preco].notna().sum() == 0:
            vencedores.append(gdf.iloc[0])
            continue

        gdf_sorted = gdf.sort_values(by=[col_preco], ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
