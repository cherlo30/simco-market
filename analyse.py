#!/usr/bin/env python3
"""Analyse l'historique collecte.

  python3 analyse.py 146            profil de la citrouille
  python3 analyse.py 146 2 66 13    plusieurs ressources

Sort : profil horaire (UTC), amplitude journaliere, et le prix fixe
conseille pour un contrat.
"""
import csv, glob, os, statistics, sys
from collections import defaultdict

NOMS = {146: "Citrouille", 2: "Eau", 66: "Semences", 13: "Transport",
        147: "Jack o'lantern", 149: "Soupe de citrouille", 29: "Recherche vegetale",
        1: "Energie", 6: "Grain", 72: "Canne a sucre", 40: "Coton"}

FRAIS_BOURSE = 0.04       # 4 % preleves sur une vente a l'echange
TRANSPORT_PRIX = 0.365    # prix d'une unite de transport


def charger(kind):
    pts = []
    for f in sorted(glob.glob(os.path.join("data", "ticker", "*.csv"))):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if int(r["kind"]) == kind:
                    pts.append((r["ts"], float(r["price"])))
    return pts


def analyse(kind, tr=1.0):
    pts = charger(kind)
    nom = NOMS.get(kind, f"ressource {kind}")
    if len(pts) < 12:
        print(f"\n{nom} : {len(pts)} releves — pas encore assez, reviens dans quelques heures.")
        return
    print(f"\n{'='*62}\n{nom}  ({len(pts)} releves)\n{'='*62}")

    par_heure = defaultdict(list)
    par_jour = defaultdict(list)
    for ts, p in pts:
        par_heure[int(ts[11:13])].append(p)
        par_jour[ts[:10]].append(p)

    print("\nProfil horaire (UTC)")
    print(f"{'h':>4}{'median':>10}{'min':>9}{'max':>9}{'n':>6}   ecart a la mediane globale")
    glob_med = statistics.median([p for _, p in pts])
    for h in sorted(par_heure):
        v = par_heure[h]
        m = statistics.median(v)
        ecart = (m / glob_med - 1) * 100
        barre = ("+" if ecart >= 0 else "-") * min(int(abs(ecart) * 4), 30)
        print(f"{h:>4}{m:>10.3f}{min(v):>9.3f}{max(v):>9.3f}{len(v):>6}   {ecart:+6.2f}% {barre}")

    print("\nPar jour")
    print(f"{'date':>12}{'bas':>9}{'haut':>9}{'amplitude':>11}{'median':>9}")
    for d in sorted(par_jour):
        v = par_jour[d]
        lo, hi = min(v), max(v)
        print(f"{d:>12}{lo:>9.3f}{hi:>9.3f}{(hi/lo-1)*100:>10.1f}%{statistics.median(v):>9.3f}")

    creux = [min(par_jour[d]) for d in par_jour if len(par_jour[d]) >= 12]
    if creux:
        ref = min(creux) if len(creux) < 3 else statistics.median(creux)
        transport_contrat = 0.5 * tr * TRANSPORT_PRIX
        transport_bourse = tr * TRANSPORT_PRIX
        for remise in (0.0, 0.01, 0.02, 0.03):
            prix = round(ref * (1 - remise), 2)
            net = prix - transport_contrat
            equiv = (net + transport_bourse) / (1 - FRAIS_BOURSE)
            print(f"  prix fixe {prix:>7.2f}  -> tu encaisses {net:>7.3f}"
                  f"   = une vente en bourse a {equiv:>6.2f}")
        print(f"\n  Reference : creux journalier median = {ref:.3f}")
        print("  Un prix fixe sous le creux est accepte a n'importe quelle heure.")


# ---------------------------------------------------------------- volume
def volume(kind):
    """Reconstitue le volume echange en comparant deux photos successives
    du carnet : un ordre dont la quantite baisse ou qui disparait a ete achete."""
    import glob, gzip, csv as _csv
    from collections import defaultdict
    photos = defaultdict(dict)            # ts -> {order_id: (prix, qte)}
    for f in sorted(glob.glob("data/flow/*/*.csv.gz")) + sorted(glob.glob("data/flow/*.csv.gz")):
        with gzip.open(f, "rt") as fh:
            for r in _csv.DictReader(fh):
                if int(r["kind"]) == kind:
                    photos[r["ts"]][r["order_id"]] = (float(r["price"]), int(r["quantity"]))
    ts = sorted(photos)
    if len(ts) < 2:
        print(f"\n{NOMS.get(kind, kind)} : {len(ts)} photo(s) — il en faut au moins 2.")
        return
    print(f"\n{'='*62}\nVolume echange — {NOMS.get(kind, kind)}\n{'='*62}")
    print(f"{'periode':>22}{'unites':>12}{'valeur':>14}{'prix moy':>11}")
    tot_q = tot_v = 0
    for a, b in zip(ts, ts[1:]):
        q = v = 0
        for oid, (p, qa) in photos[a].items():
            qb = photos[b].get(oid, (p, 0))[1]
            if qa > qb:
                q += qa - qb
                v += (qa - qb) * p
        tot_q += q; tot_v += v
        print(f"{b[11:16]:>22}{q:>12,}{v:>14,.0f}{(v/q if q else 0):>11.3f}")
    heures = (len(ts) - 1) * 20 / 60.0
    print(f"\n  total {tot_q:,} unites pour {tot_v:,.0f} $ sur ~{heures:.1f} h")
    if heures:
        print(f"  soit {tot_q/heures:,.0f} unites/heure, {tot_q/heures*24:,.0f}/jour")
        print("  (une baisse de quantite peut aussi etre une annulation : "
              "chiffre plafond, pas exact)")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:]]
    vol = "--volume" in args
    kinds = [int(a) for a in args if a.lstrip("-").isdigit()] or [146]
    for k in kinds:
        analyse(k, tr=1.0 if k == 146 else 0.1)
        if vol:
            volume(k)
