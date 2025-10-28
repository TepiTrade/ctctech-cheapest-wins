# -*- coding: utf-8 -*-
import pandas as pd

def _col(gdf: pd.DataFrame, candidatos):
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        if nome in cols_lower:
            return cols_lower[nome]
    return None

def _iter_groups(groups):
    if isinstance(groups, pd.core.groupby.generic.DataFrameGroupBy):
        return ((k, df) for k, df in groups)
    if isinstance(groups, pd.DataFrame):
        return [(None, groups)]
    try:
        it = iter(groups)
        first = next(iter(groups))
        if isinstance(first, pd.DataFrame):
            return enumerate(groups)
    except Exception:
        pass
    return []

def pick_winners(groups, cfg):
    vencedores = []
    for _, gdf in _iter_groups(groups):  # <<< evita o erro "cannot unpack non-iterable"
        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])
        if hasattr(col_preco, "name"):
            col_preco = col_preco.name
        if col_preco is None or col_preco not in gdf.columns:
            nums = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = nums[0] if nums else None
        if col_preco is None or gdf.empty:
            vencedores.append(gdf.iloc[0] if not gdf.empty else pd.Series())
            continue
        gdf[col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')
        if gdf[col_preco].notna().sum() == 0:
            vencedores.append(gdf.iloc[0])
            continue
        gdf_sorted = gdf.sort_values(by=[col_preco], ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])
    return pd.DataFrame(vencedores).reset_index(drop=True)
