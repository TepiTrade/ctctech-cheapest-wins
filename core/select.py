# -*- coding: utf-8 -*-
import pandas as pd

def _col(gdf: pd.DataFrame, candidatos):
    """Retorna o nome exato da coluna existente no DF, dado candidatos em minúsculas."""
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        if nome in cols_lower:
            return cols_lower[nome]
    return None

def _iter_groups(groups):
    """Normaliza 'groups' para um iterável de DataFrames."""
    # groupby(DataFrame).groups
    if isinstance(groups, pd.core.groupby.generic.DataFrameGroupBy):
        return ((k, df) for k, df in groups)
    # DataFrame único
    if isinstance(groups, pd.DataFrame):
        return [(None, groups)]
    # lista/tupla de DataFrames
    try:
        it = iter(groups)
        peek = next(iter(groups))
        if isinstance(peek, pd.DataFrame):
            return enumerate(groups)
    except Exception:
        pass
    # forma desconhecida → nada a processar
    return []

def pick_winners(groups, cfg):
    """Escolhe o item mais barato em cada grupo. Aceita groupby, DF único ou lista de DFs."""
    vencedores = []

    for _, gdf in _iter_groups(groups):
        # 1) Detecta coluna de preço (PT/EN)
        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])
        if hasattr(col_preco, "name"):
            col_preco = col_preco.name

        # 2) Fallback: primeira coluna numérica
        if col_preco is None or col_preco not in gdf.columns:
            num_cols = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = num_cols[0] if num_cols else None

        # 3) Se ainda não houver, pega a primeira linha
        if col_preco is None or gdf.empty:
            vencedores.append(gdf.iloc[0] if not gdf.empty else pd.Series())
            continue

        # 4) Converte e ordena
        gdf[col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')
        if gdf[col_preco].notna().sum() == 0:
            vencedores.append(gdf.iloc[0])
            continue

        gdf_sorted = gdf.sort_values(by=[col_preco], ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
