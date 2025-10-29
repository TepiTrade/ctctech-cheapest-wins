# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
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
    """
    Só produz (chave, DataFrame) ou DataFrame. Ignora qualquer outro tipo.
    Esta função garante que apenas objetos válidos do Pandas saiam.
    """
    # 1) GroupBy -> (k, df)
    try:
        from pandas.core.groupby.generic import DataFrameGroupBy
        if isinstance(groups, DataFrameGroupBy):
            for k, df in groups:
                if isinstance(df, pd.DataFrame):
                    yield (k, df)
            return
    except Exception:
        pass
        
    # 2) DataFrame único
    if isinstance(groups, pd.DataFrame):
        yield (None, groups); return
        
    # 3) Lista de DFs
    if isinstance(groups, list) and all(isinstance(df, pd.DataFrame) for df in groups):
        for i, df in enumerate(groups):
            yield (i, df)
        return
        
    # 4) Iterável genérico: só aceita (k, df) ou df; ignora ints/strings/etc.
    if isinstance(groups, Iterable):
        for item in groups:
            # item é uma tupla de 2 e o segundo elemento é um DataFrame?
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], pd.DataFrame):
                yield item
            # item é um DataFrame?
            elif isinstance(item, pd.DataFrame):
                yield (None, item)

def _as_df_iter(groups) -> Generator[pd.DataFrame, None, None]:
    """Converte o resultado de _iter_groups em uma iteração de DataFrames puros."""
    for item in _iter_groups(groups):
        if isinstance(item, tuple) and len(item) == 2:
            # Desempacota (chave, df) e pega apenas o df
            _, df = item
        else:
            # Se for apenas um item (espera-se que seja um DataFrame)
            df = item
            
        if isinstance(df, pd.DataFrame):
            yield df

# --- CORREÇÃO NA FUNÇÃO PRINCIPAL ---
def pick_winners(groups, cfg):
    """Escolhe o mais barato em cada grupo. Tolerante a PT/EN."""
    vencedores = []
    
    # MUDANÇA CRUCIAL: A iteração deve usar APENAS 'gdf' e chamar '_as_df_iter',
    # que é quem faz a limpeza e garante que só DataFrames sejam processados.
    # A linha original com erro "for _, gdf in _iter_groups(groups):" foi removida.
    for gdf in _as_df_iter(groups): # <-- Esta linha agora é a iteração correta
        if gdf is None or gdf.empty:
            continue

        # 1) Detecção de Coluna de Preço
        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])
        if col_preco is None or col_preco not in gdf.columns:
            nums = [c for c in gdf.columns if pd.api.types.is_numeric_dtype(gdf[c])]
            col_preco = nums[0] if nums else None

        # 2) Fallback para coluna inválida
        if col_preco is None:
            vencedores.append(gdf.iloc[0]); continue

        # 3) Conversão e Filtragem
        # Usando .loc para evitar SettingWithCopyWarning
        gdf.loc[:, col_preco] = pd.to_numeric(gdf[col_preco], errors='coerce')
        gdf_valid = gdf.dropna(subset=[col_preco])
        
        if gdf_valid.empty:
            vencedores.append(gdf.iloc[0]); continue

        # 4) Ordenar e Selecionar
        gdf_sorted = gdf_valid.sort_values(by=col_preco, ascending=True, kind="mergesort")
        vencedores.append(gdf_sorted.iloc[0])

    return pd.DataFrame(vencedores).reset_index(drop=True)
