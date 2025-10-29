# -*- coding: utf-8 -*-
import pandas as pd
from typing import Any, Tuple, Generator, Iterable

def _col(gdf: pd.DataFrame, candidatos):
    """Retorna o nome exato da coluna se existir (case-insensitive)."""
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        n = str(nome).lower()
        if n in cols_lower:
            return cols_lower[n]
    return None

def _iter_groups(groups) -> Generator[Tuple[Any, pd.DataFrame] | pd.DataFrame, None, None]:
    """Entrega (chave, DataFrame) ou apenas DataFrame; ignora tipos inválidos."""
    try:
        from pandas.core.groupby.generic import DataFrameGroupBy
        if isinstance(groups, DataFrameGroupBy):
            for k, df in groups:
                if isinstance(df, pd.DataFrame):
                    yield (k, df)
            return
    except Exception:
        pass

    if isinstance(groups, pd.DataFrame):
        yield (None, groups)
        return

    if isinstance(groups, list) and all(isinstance(df, pd.DataFrame) for df in groups):
        for i, df in enumerate(groups):
            yield (i, df)
        return

    if isinstance(groups, Iterable):
        for item in groups:
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], pd.DataFrame):
                yield item
            elif isinstance(item, pd.DataFrame):
                yield (None, item)

def _as_df_iter(groups) -> Generator[pd.DataFrame, None, None]:
    """Itera somente DataFrames, independente do formato de entrada."""
    for item in _iter_groups(groups):
        df = item[1] if isinstance(item, tuple) and len(item) == 2 else item
        if isinstance(df, pd.DataFrame):
            yield df

def pick_winners(groups, cfg):
    """Seleciona o item mais barato em cada grupo."""
    vencedores = []
    for gdf in _as_df_iter(groups):
        if gdf is None or gdf.empty:
            continue

        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])
        if col_preco is None or col_preco not in gdf.columns:
            nums = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = nums[0] if nums else None

        if col_preco is None:
            vencedores.append(gdf.iloc[0])
            continue

        gdf.loc[:, col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')
        gdf_valid = gdf.dropna(subset=[col_preco])
        if gdf_valid.empty:
            vencedores.append(gdf.iloc[0])
            continue

        gdf_sorted = gdf_valid.sort_values(by=col_preco, ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
