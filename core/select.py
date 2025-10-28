# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def _col(gdf: pd.DataFrame, candidatos):
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        n = nome.lower()
        if n in cols_lower:
            return cols_lower[n]
    return None

def _iter_groups(groups):
    if isinstance(groups, pd.core.groupby.generic.DataFrameGroupBy):
        return ((k, df) for k, df in groups)
    if isinstance(groups, pd.DataFrame):
        return [(None, groups)]
    if isinstance(groups, list) and all(isinstance(df, pd.DataFrame) for df in groups):
        return enumerate(groups)
    try:
        return iter(groups)
    except Exception:
        return iter([])

def pick_winners(groups, cfg):
    vencedores = []
    for _, gdf in _iter_groups(groups):
        if gdf is None or not isinstance(gdf, pd.DataFrame) or gdf.empty:
            continue

        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])

        if col_preco is None or col_preco not in gdf.columns:
            nums = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = nums[0] if nums else None

        if col_preco is None:
            vencedores.append(gdf.iloc[0])
            continue

        gdf[col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')

        if gdf[col_preco].notna().sum() == 0:
            vencedores.append(gdf.iloc[0])
            continue

        gdf_valid = gdf.dropna(subset=[col_preco])
        if gdf_valid.empty:
            vencedores.append(gdf.iloc[0])
            continue

        gdf_sorted = gdf_valid.sort_values(by=col_preco, ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
