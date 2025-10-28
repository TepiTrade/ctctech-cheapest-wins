import os, sys, argparse, pandas as pd
from adapters.csv_adapter import load_sources
from core.normalize import normalize_record_df
from core.match import build_groups
from core.select import pick_winners
from core.woo import WooClient, upsert_products
import yaml

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = load_config()
    df = load_sources(cfg["sources"])
    if df.empty:
        print("Nenhum item carregado das fontes."); return

    df = normalize_record_df(df, cfg)
    groups = build_groups(df, min_similarity=cfg.get("min_similarity", 92))
    winners = pick_winners(groups, cfg)

    print(f"Itens totais: {len(df)}  | Grupos: {len(groups)}  | Vencedores: {len(winners)}")

    if args.dry_run:
        print(winners.head(20).to_string(index=False))
        return

    base_url = os.getenv("WC_BASE_URL")
    ck = os.getenv("WC_CK")
    cs = os.getenv("WC_CS")
    button_text = os.getenv("DEFAULT_BUTTON_TEXT", "Comprar")
    default_currency = os.getenv("DEFAULT_CURRENCY", "BRL")

    if not all([base_url, ck, cs]):
        print("Secrets faltando: WC_BASE_URL, WC_CK, WC_CS"); sys.exit(1)

    wc = WooClient(base_url, ck, cs, default_currency, button_text)
    upsert_products(wc, winners, cfg)

if __name__ == "__main__":
    main()
