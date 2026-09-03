#!/usr/bin/env python3
"""Photographie les carnets d'ordres de Sim Companies.

Deux sorties :

  data/book/AAAA-MM-JJ.csv        agregats pour TOUTES les ressources
  data/flow/AAAA-MM/AAAA-MM-JJ.csv.gz   ordres individuels de TOUTES les
                                  ressources, ce qui permet de reconstituer le
                                  volume reellement echange en comparant deux photos

L'age median des 20 offres les moins cheres mesure la vitesse d'ecoulement :
sous 2 h le produit part vite, au-dela de 20 h il stagne.
"""
import csv, gzip, json, os, statistics, time, urllib.request
from datetime import datetime, timezone

REALM = 0
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
BOOK = "https://www.simcompanies.com/api/v3/market/%d/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/1.0)"
DELAY = 0.8              # secondes entre deux requetes

# Detail ordre par ordre sur TOUTES les ressources.
# On ne garde que les N offres les moins cheres : au-dela, les ordres sont poses
# tres au-dessus du marche et ne se negocient jamais. Mets None pour tout garder
# (compte alors ~600 Mo par mois au lieu de ~260).
MAX_ORDRES = 150


def fetch(url, tries=3):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            time.sleep(2 * (i + 1))
    print(f"  ignore {url} : {last}")
    return None


def age_h(iso, now):
    try:
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return round((now - t).total_seconds() / 3600.0, 3)
    except Exception:
        return ""


def main():
    tick = fetch(TICKER) or []
    kinds = sorted({r["kind"] for r in tick})
    if not kinds:
        raise SystemExit("ticker vide, on arrete")

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    jour = now.strftime("%Y-%m-%d")
    os.makedirs("data/book", exist_ok=True)
    os.makedirs("data/flow", exist_ok=True)

    mois = now.strftime("%Y-%m")
    os.makedirs(f"data/flow/{mois}", exist_ok=True)
    p_agg = f"data/book/{jour}.csv"
    p_flow = f"data/flow/{mois}/{jour}.csv.gz"
    neuf_agg = not os.path.exists(p_agg)
    neuf_flow = not os.path.exists(p_flow)

    f_agg = open(p_agg, "a", newline="")
    f_flow = gzip.open(p_flow, "at", newline="")
    w_agg, w_flow = csv.writer(f_agg), csv.writer(f_flow)
    if neuf_agg:
        w_agg.writerow(["ts", "kind", "best", "qty_best", "n_orders",
                        "total_qty", "qty_within_5pct", "median_age_h"])
    if neuf_flow:
        w_flow.writerow(["ts", "kind", "order_id", "price", "quantity", "quality",
                         "seller_id", "posted"])

    faits = 0
    try:
        for k in kinds:
            book = fetch(BOOK % (REALM, k))
            time.sleep(DELAY)
            if not book:
                continue
            book.sort(key=lambda x: x["price"])
            best = book[0]["price"]
            w_agg.writerow([
                stamp, k, best,
                sum(x["quantity"] for x in book if x["price"] == best),
                len(book),
                sum(x["quantity"] for x in book),
                sum(x["quantity"] for x in book if x["price"] <= best * 1.05),
                (lambda a: round(statistics.median(a), 3) if a else "")(
                    [v for v in (age_h(x.get("posted") or "", now) for x in book[:20]) if v != ""]),
            ])
            gardes = book if MAX_ORDRES is None else book[:MAX_ORDRES]
            if True:
                for x in gardes:
                    w_flow.writerow([stamp, k, x["id"], x["price"], x["quantity"],
                                     x.get("quality", 0),
                                     (x.get("seller") or {}).get("id", ""),
                                     x.get("posted", "")])
            faits += 1
    finally:
        f_agg.close()
        f_flow.close()
    lim = "complet" if MAX_ORDRES is None else f"{MAX_ORDRES} offres les moins cheres"
    print(f"{stamp} : {faits}/{len(kinds)} carnets, detail {lim}")


if __name__ == "__main__":
    main()
