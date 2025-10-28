import pandas as pd

def to_brl(amount, currency, rates):
    currency = (currency or "BRL").upper()
    rate = rates.get(currency, 1.0)
    return float(amount) * float(rate)

def total_price_row(row, rates):
    base = to_brl(row.get("price",0.0), row.get("currency","BRL"), rates)
    ship = to_brl(row.get("shipping",0.0), row.get("currency","BRL"), rates)
    fee_pct = float(row.get("fee_percent", 0.0)) / 100.0
    fee = base * fee_pct
    return base + ship + fee

def pick_winners(groups: dict, cfg: dict) -> pd.DataFrame:
    winners = []
    rates = cfg.get("exchange_rates", {"BRL":1.0})
    priority = cfg.get("platform_priority", [])
    pr_index = {p:i for i,p in enumerate(priority)}
    for gid, gdf in groups.items():
        gdf = gdf.copy()
        gdf["total_brl"] = gdf.apply(lambda r: total_price_row(r, rates), axis=1)
        # escolher menor total
        gdf.sort_values(
            by=["total_brl", gdf["platform"].map(lambda p: pr_index.get(str(p).lower(), 9999)), "url"],
            ascending=[True, True, True],
            inplace=True
        )
        top = gdf.iloc[0].copy()
        winners.append(top)
    return pd.DataFrame(winners)
