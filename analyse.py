#!/usr/bin/env python3
"""Analyse de l'historique collecte.

  python3 analyse.py 146              profil horaire de la citrouille
  python3 analyse.py 146 2 66 13      plusieurs ressources
  python3 analyse.py 146 --volume     + le volume reellement echange
  python3 analyse.py --top            les ressources les plus liquides
  python3 analyse.py 146 --q 4        la citrouille en qualite 4 etoiles

Chaque commande affiche aussi l'echelle de qualite : le prix de la ressource
a chaque niveau d'etoiles, et l'ecart avec la qualite 0.
"""
import csv, glob, gzip, os, statistics, sys
from collections import defaultdict

NOMS = {146: "Citrouille", 2: "Eau", 66: "Semences", 13: "Transport",
        147: "Jack o'lantern", 149: "Soupe de citrouille", 29: "Recherche vegetale",
        1: "Energie", 6: "Grain", 72: "Canne a sucre", 40: "Coton", 120: "Legumes",
        3: "Pommes", 4: "Oranges", 44: "Sable", 106: "Bois"}

FRAIS = 0.04            # frais de bourse sur une vente a l'echange
TRANSPORT = 0.365       # prix d'une unite de transport


def nom(k):
    return NOMS.get(k, f"ressource {k}")


def lire_prix(kind, qual=0):
    pts = []
    if qual == 0:
        for f in sorted(glob.glob("data/ticker/*.csv")):
            with open(f) as fh:
                for r in csv.DictReader(fh):
                    if int(r["kind"]) == kind:
                        pts.append((r["ts"], float(r["price"])))
    for f in sorted(glob.glob("data/book/*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if int(r["kind"]) == kind and int(r.get("quality") or 0) == qual:
                    pts.append((r["ts"], float(r["best"])))
    for f in sorted(glob.glob("data/hourly/*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if (int(r["kind"]) == kind and int(r.get("quality") or 0) == qual
                        and r["cloture"]):
                    pts.append((r["heure"] + ":30:00Z", float(r["cloture"])))
    return sorted(set(pts))


def echelle_qualite(kind):
    """Prix le plus recent par niveau de qualite."""
    dern = {}
    for f in sorted(glob.glob("data/book/*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if int(r["kind"]) == kind and r.get("best"):
                    dern[int(r.get("quality") or 0)] = (r["ts"], float(r["best"]),
                                                        int(r["total_qty"]))
    if len(dern) < 2:
        return
    base = dern.get(0, (None, None, None))[1]
    print(f"\nEchelle de qualite — {nom(kind)}")
    print(f"{'Q':>3}{'prix':>10}{'vs Q0':>9}{'profondeur':>13}")
    for q in sorted(dern):
        _, p, tot = dern[q]
        ecart = f"{(p/base-1)*100:+.1f}%" if base else ""
        print(f"{q:>3}{p:>10.3f}{ecart:>9}{tot:>13,}")


def profil(kind, tr=1.0, qual=0):
    pts = lire_prix(kind, qual)
    if len(pts) < 8:
        print(f"\n{nom(kind)} : {len(pts)} releves — reviens dans quelques heures.")
        return
    print(f"\n{'='*66}\n{nom(kind)}  —  {len(pts)} releves\n{'='*66}")

    par_h, par_j = defaultdict(list), defaultdict(list)
    for ts, p in pts:
        par_h[int(ts[11:13])].append(p)
        par_j[ts[:10]].append(p)
    med = statistics.median([p for _, p in pts])

    print("\nProfil horaire (UTC)")
    print(f"{'h':>4}{'median':>10}{'bas':>9}{'haut':>9}{'n':>6}   ecart")
    for h in sorted(par_h):
        v = par_h[h]
        m = statistics.median(v)
        e = (m / med - 1) * 100
        barre = ("+" if e >= 0 else "-") * min(int(abs(e) * 4), 28)
        print(f"{h:>4}{m:>10.3f}{min(v):>9.3f}{max(v):>9.3f}{len(v):>6}  {e:+6.2f}% {barre}")

    print("\nPar jour")
    print(f"{'date':>12}{'bas':>9}{'haut':>9}{'amplitude':>11}{'median':>9}")
    for d in sorted(par_j):
        v = par_j[d]
        print(f"{d:>12}{min(v):>9.3f}{max(v):>9.3f}{(max(v)/min(v)-1)*100:>10.1f}%"
              f"{statistics.median(v):>9.3f}")

    creux = [min(par_j[d]) for d in par_j if len(par_j[d]) >= 8]
    if creux:
        ref = statistics.median(creux) if len(creux) >= 3 else min(creux)
        print(f"\nPrix fixe conseille pour un contrat  (creux journalier median {ref:.3f})")
        for rem in (0.0, 0.01, 0.02, 0.03):
            px = round(ref * (1 - rem), 2)
            net = px - 0.5 * tr * TRANSPORT
            equiv = (net + tr * TRANSPORT) / (1 - FRAIS)
            print(f"  {px:>7.2f}  -> tu encaisses {net:>7.3f}"
                  f"   = une vente en bourse a {equiv:>6.2f}")


def volume(kind):
    par_h = defaultdict(lambda: [0.0, 0.0])
    for f in sorted(glob.glob("data/tape/*/*.csv.gz")):
        with gzip.open(f, "rt") as fh:
            for r in csv.DictReader(fh):
                if int(r["kind"]) != kind or r["reprise"] == "1":
                    continue
                if r["evt"] in ("C", "X") and r["delta"]:
                    d = float(r["delta"])
                    if d > 0:
                        h = par_h[r["ts"][:13]]
                        h[0] += d
                        h[1] += d * float(r["price"] or 0)
    for f in sorted(glob.glob("data/hourly/*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if int(r["kind"]) == kind and r["volume"]:
                    h = par_h.setdefault(r["heure"], [0.0, 0.0])
                    if h[0] == 0:
                        h[0], h[1] = float(r["volume"]), float(r["valeur"] or 0)
    if not par_h:
        print(f"\n{nom(kind)} : pas encore de mouvements enregistres.")
        return
    print(f"\n{'='*66}\nVolume echange — {nom(kind)}\n{'='*66}")
    print(f"{'heure':>16}{'unites':>12}{'valeur':>14}{'prix moyen':>13}")
    tq = tv = 0
    for h in sorted(par_h):
        q, v = par_h[h]
        tq += q; tv += v
        print(f"{h:>16}{q:>12,.0f}{v:>14,.0f}{(v/q if q else 0):>13.3f}")
    n = len(par_h)
    print(f"\n  total {tq:,.0f} unites, {tv:,.0f} $ sur {n} heure(s)")
    print(f"  soit {tq/n:,.0f} unites/heure, {tq/n*24:,.0f}/jour")
    print("  (une baisse de quantite peut aussi etre une annulation : plafond)")


def top():
    vol = defaultdict(float)
    for f in sorted(glob.glob("data/hourly/*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if r["valeur"]:
                    vol[int(r["kind"])] += float(r["valeur"])
    if not vol:
        print("pas encore d'agregats horaires — lance rollup.py")
        return
    print(f"\n{'='*66}\nRessources les plus echangees (valeur)\n{'='*66}")
    for k, v in sorted(vol.items(), key=lambda x: -x[1])[:25]:
        print(f"{nom(k):<22}{v:>16,.0f} $")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--top" in args:
        top(); sys.exit()
    kinds = [int(a) for a in args if a.lstrip("-").isdigit()] or [146]
    qual = 0
    for i, a_ in enumerate(args):
        if a_ == "--q" and i + 1 < len(args):
            qual = int(args[i + 1])
    for k in kinds:
        profil(k, tr=1.0 if k in (146, 44) else 0.1, qual=qual)
        echelle_qualite(k)
        if "--volume" in args:
            volume(k)
