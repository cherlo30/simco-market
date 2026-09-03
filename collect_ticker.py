#!/usr/bin/env python3
"""Releve le ticker complet de Sim Companies et l'ajoute au CSV du jour.

Une seule requete ramene le prix des ~140 ressources.
Sortie : data/ticker/YYYY-MM-DD.csv
"""
import csv, json, os, sys, time, urllib.request
from datetime import datetime, timezone

REALM = 0
URL = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/1.0)"
OUT = os.path.join("data", "ticker")


def fetch(url, tries=4):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except Exception as e:                      # 429, 5xx, reseau
            last = e
            time.sleep(2 * (i + 1))
    raise SystemExit(f"echec apres {tries} tentatives : {last}")


def main():
    rows = fetch(URL)
    now = datetime.now(timezone.utc)
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, now.strftime("%Y-%m-%d") + ".csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "kind", "price", "is_up"])
        stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        for r in rows:
            w.writerow([stamp, r["kind"], r["price"], int(bool(r.get("is_up")))])
    print(f"{stamp} : {len(rows)} ressources -> {path}")


if __name__ == "__main__":
    main()
