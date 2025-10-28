# -*- coding: utf-8 -*-
import pandas as pd

def _col(gdf, candidatos):
    """Retorna o nome exato da coluna existente no DF, dado uma lista de candidatos."""
    cols_lower = {c.lower(): c for c in gdf.columns}
    for nome in candidatos:
        if nome in cols_lower:
            return cols_lower[nome]
    return None

def pick_winners(groups, cfg):
    """
    Escolhe o item mais barato dentro de cada grupo.
    Tolera cabeçalhos em PT/EN: preço/preco/price/valor.
    """
    vencedores = []

    for _, gdf in groups:
        # Detecta a coluna de preço com tolerância a variações
        col_preco = _col(gdf, ['preço', 'preco', 'price', 'valor'])

        # Se não achar a coluna de preço, evita quebrar: pega a primeira linha do grupo
        if not col_preco:
            vencedores.append(gdf.iloc[0])
            continue

        # Ordena pelo preço (asc) de forma estável e pega o mais barato
        gdf_ordenado = gdf.sort_values(by=col_preco, ascending=True, kind='mergesort')
        vencedores.append(gdf_ordenado.iloc[0])

    # Retorna dataframe com os vencedores
    if len(vencedores) == 0:
        return pd.DataFrame(columns=['vazio'])
    return pd.DataFrame(vencedores)
