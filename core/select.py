# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from typing import Any, Tuple, Generator

def _col(gdf: pd.DataFrame, candidatos):
    cols_lower = {str(c).lower(): c for c in gdf.columns}
    for nome in candidatos:
        n = str(nome).lower()
        if n in cols_lower:
            return cols_lower[n]
    return None

def _iter_groups(groups) -> Generator[Tuple[Any, pd.DataFrame] | pd.DataFrame, None, None]:
    """Itera apenas formatos esperados. Nunca devolve inteiros para desempacotar."""
    # 1) GroupBy → (chave, df)
    try:
        from pandas.core.groupby.generic import DataFrameGroupBy
        if isinstance(groups, DataFrameGroupBy):
            for k, df in groups:
                yield (k, df)
            return
    except ImportError:
        pass
    # 2) DataFrame único
    if isinstance(groups, pd.DataFrame):
        yield (None, groups); return
    # 3) Lista de DFs
    if isinstance(groups, list) and all(isinstance(df, pd.DataFrame) for df in groups):
        for i, df in enumerate(groups):
            yield (i, df)
        return
    # 4) Iterável genérico: só cede itens que sejam (k, df) ou df
    try:
        for item in groups:
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], pd.DataFrame):
                yield item
            elif isinstance(item, pd.DataFrame):
                yield (None, item)
            # ignora qualquer outra coisa (ints, strings, etc.)
    except TypeError:
        return

def _as_df_iter(groups):
    """Filtra para devolver só DataFrames."""
    for item in _iter_groups(groups):
        if isinstance(item, tuple) and len(item) == 2:
            _, df = item
        else:
            df = item
        if isinstance(df, pd.DataFrame):
            yield df

def pick_winners(groups, cfg):
    """Seleciona o item mais barato por grupo. Tolerante a cabeçalhos PT/EN."""
    vencedores = []
    for gdf in _as_df_iter(groups):
        if gdf is None or gdf.empty:
            continue

        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])
        if col_preco is None or col_preco not in gdf.columns:
            nums = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = nums[0] if nums else None

        if col_preco is None:
            vencedores.append(gdf.iloc[0]); continue

        gdf.loc[:, col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')
        gdf_valid = gdf.dropna(subset=[col_preco])
        if gdf_valid.empty:
            vencedores.append(gdf.iloc[0]); continue

        gdf_sorted = gdf_valid.sort_values(by=col_preco, ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
