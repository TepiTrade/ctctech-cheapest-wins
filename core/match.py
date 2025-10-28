from rapidfuzz import fuzz
import pandas as pd
from collections import defaultdict

def build_groups(df: pd.DataFrame, min_similarity: int = 92):
    # Primeiro agrupa por chave canônica
    buckets = defaultdict(list)
    for idx, row in df.iterrows():
        buckets[row["key"]].append(idx)

    # Depois tenta mesclar buckets muito próximos pelo título
    keys = list(buckets.keys())
    merged = {}
    used = set()
    gid = 0
    for i, k in enumerate(keys):
        if k in used: 
            continue
        group = set(buckets[k])
        for j in range(i+1, len(keys)):
            k2 = keys[j]
            if k2 in used:
                continue
            # similaridade por título normalizado
            t1 = " ".join(df.loc[list(group), "title_norm"].head(1).tolist())
            t2 = df.loc[buckets[k2][0], "title_norm"]
            if fuzz.token_set_ratio(t1, t2) >= min_similarity:
                group.update(buckets[k2])
                used.add(k2)
        merged[gid] = list(group)
        gid += 1
    # retorna dict gid -> dataframe do grupo
    groups = {g: df.loc[idxs].copy() for g, idxs in merged.items()}
    return groups
