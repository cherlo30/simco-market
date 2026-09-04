#!/usr/bin/env python3
"""Synthese et purge.

- construit des agregats HORAIRES a partir des prix et des mouvements
  (ouverture, plus haut, plus bas, cloture, volume echange, profondeur)
- efface la bande de mouvements de plus de RETENTION jours

Les agregats pesent quelques megaoctets par an : on peut les garder pour
toujours. La bande brute, elle, ne sert que pour l'analyse fine recente.
"""
import csv, glob, gzip, os, statistics, sys, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

RETENTION = int(os.environ.get("RETENTION_JOURS", "30"))
SORTIE = "data/hourly"


def heure(ts):
    return ts[:13]          # AAAA-MM-JJTHH


def enregistrements(fh):
    """Lit un CSV en ignorant les lignes qui n'ont pas le bon nombre de
    colonnes. Un fichier ecrit par deux versions du collecteur en contient :
    les interpreter avec le mauvais en-tete produirait des prix absurdes."""
    r = csv.reader(fh)
    try:
        entete = next(r)
    except StopIteration:
        return
    n = len(entete)
    for ligne in r:
        if len(ligne) == n:
            yield dict(zip(entete, ligne))


def nombre(x):
    """Le jeu ecrit parfois 'sold out' au lieu d'un prix. On l'ignore."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main():
    prix = defaultdict(list)        # (heure, kind, qualite) -> [prix]
    vol = defaultdict(float)        # unites vendues
    val = defaultdict(float)        # valeur echangee
    prof = defaultdict(list)        # profondeur a -5 %

    for f in sorted(glob.glob("data/ticker/*.csv")):
        with open(f) as fh:
            for r in enregistrements(fh):
                p = nombre(r["price"])
                if p is not None:
                    prix[(heure(r["ts"]), int(r["kind"]), 0)].append(p)

    for f in sorted(glob.glob("data/book/*.csv")):
        with open(f) as fh:
            for r in enregistrements(fh):
                d = nombre(r.get("qty_within_5pct"))
                if d is not None:
                    q = int(r.get("quality") or 0)
                    prof[(heure(r["ts"]), int(r["kind"]), q)].append(d)
                    b = nombre(r.get("best"))
                    if b is not None:
                        prix[(heure(r["ts"]), int(r["kind"]), q)].append(b)

    for f in sorted(glob.glob("data/tape/*/*.csv.gz")):
        with gzip.open(f, "rt") as fh:
            for r in enregistrements(fh):
                if r["evt"] in ("C", "X") and r["reprise"] != "1" and r["delta"]:
                    d = nombre(r["delta"])
                    if d and d > 0:
                        k = (heure(r["ts"]), int(r["kind"]),
                             int(r.get("quality") or 0))
                        vol[k] += d
                        val[k] += d * (nombre(r["price"]) or 0)

    os.makedirs(SORTIE, exist_ok=True)
    par_mois = defaultdict(list)
    for cle in sorted(set(prix) | set(vol) | set(prof)):
        h, k, q = cle
        p = prix.get(cle) or []
        par_mois[h[:7]].append([
            h, k, q,
            p[0] if p else "", max(p) if p else "", min(p) if p else "",
            p[-1] if p else "", len(p),
            round(vol.get(cle, 0)), round(val.get(cle, 0)),
            round(statistics.median(prof[cle])) if prof.get(cle) else "",
        ])

    for mois, lignes in par_mois.items():
        with open(f"{SORTIE}/{mois}.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["heure", "kind", "quality", "ouverture", "haut", "bas",
                        "cloture", "n_releves", "volume", "valeur",
                        "profondeur_5pct"])
            w.writerows(lignes)
        print(f"{SORTIE}/{mois}.csv : {len(lignes)} lignes")

    # purge de la bande ancienne
    limite = (datetime.now(timezone.utc) - timedelta(days=RETENTION)).strftime("%Y-%m-%d")
    efface = 0
    for f in glob.glob("data/tape/*/*.csv.gz"):
        jour = os.path.basename(f)[:10]
        if jour < limite:
            os.remove(f); efface += 1
    if efface:
        print(f"{efface} fichier(s) de bande de plus de {RETENTION} jours effaces")


if __name__ == "__main__":
    main()
