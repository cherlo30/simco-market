#!/usr/bin/env python3
"""Convertit l'ancienne collecte vers le nouveau format, une seule fois.

ANCIEN                          NOUVEAU
  data/ticker/JJ.csv              data/horaire/AAAA-MM.csv
  data/book/JJ.csv        --->    data/volume/<ressource>/AAAA-MM.csv
  data/tape/AAAA-MM/JJ.csv.gz
  data/hourly/AAAA-MM.csv

Ce qui est recupere :
  - les prix (ouverture, plus haut, plus bas, cloture) heure par heure
  - la profondeur du carnet et le nombre d'offres
  - le volume vendu par palier de prix, pour les baisses partielles

Ce qui ne l'est pas :
  - les disparitions completes d'offres. L'ancien format ne gardait pas de
    quoi savoir s'il restait moins cher ailleurs au moment ou l'offre a
    disparu : impossible de trancher entre vente et retrait apres coup.
    On les laisse de cote plutot que d'inventer un chiffre.

Ensuite les anciens dossiers sont effaces.
"""
import csv, glob, gzip, io, os, shutil, sys
from collections import defaultdict

EN_HORAIRE = ["heure", "kind", "quality", "ouverture", "haut", "bas",
              "cloture", "n_ordres", "qte_totale", "profondeur_5pct",
              "vendu", "retire"]
EN_VOLUME = ["heure", "kind", "quality", "prix", "vendu", "n_evt"]


def lignes(fh):
    """Lit un CSV dont l'en-tete ne correspond plus forcement aux lignes.

    Les anciens fichiers melangent deux formats : le collecteur a ete mis a
    jour en cours de journee pour ajouter la colonne `quality`, en 3e
    position, sans reecrire l'en-tete. Une ligne qui a une colonne de trop
    est donc une ligne du nouveau format : on la relit avec la bonne
    grille au lieu de la jeter."""
    r = csv.reader(fh)
    try:
        entete = next(r)
    except StopIteration:
        return
    entete = [c.strip() for c in entete]
    n = len(entete)
    avec_q = entete[:2] + ["quality"] + entete[2:]
    for l in r:
        if len(l) == n:
            yield dict(zip(entete, l))
        elif len(l) == n + 1 and "quality" not in entete:
            yield dict(zip(avec_q, l))


def texte_gz(chemin):
    """Lit un fichier compresse meme s'il est abime.

    Quand le collecteur est arrete en pleine ecriture, le fichier ne se
    termine pas proprement et Python refuse de le lire jusqu'au bout. On
    garde alors tout ce qui est lisible et on s'arrete la, plutot que de
    perdre le fichier entier."""
    bouts = []
    try:
        with gzip.open(chemin, "rt", errors="replace") as fh:
            while True:
                b = fh.read(1 << 20)
                if not b:
                    break
                bouts.append(b)
    except Exception:
        print(f"  {chemin} : fin de fichier abimee, on garde ce qui est lisible")
    t = "".join(bouts)
    return t[:t.rfind("\n") + 1] if "\n" in t else ""


def nb(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main():
    prix = defaultdict(list)        # (heure,kind,q) -> [prix dans l'ordre]
    etat = {}                       # (heure,kind,q) -> (n_ordres, qte, prof)
    vendu = defaultdict(float)      # (heure,kind,q)
    volp = defaultdict(lambda: [0.0, 0])   # (heure,kind,q,prix)

    for f in sorted(glob.glob("data/ticker/*.csv")):
        with open(f) as fh:
            for r in lignes(fh):
                p = nb(r.get("price"))
                if p is not None:
                    prix[(r["ts"][:13], int(r["kind"]), 0)].append(p)

    for f in sorted(glob.glob("data/book/*.csv")):
        with open(f) as fh:
            for r in lignes(fh):
                q = int(r.get("quality") or 0)
                cle = (r["ts"][:13], int(r["kind"]), q)
                p = nb(r.get("best"))
                if p is not None:
                    prix[cle].append(p)
                etat[cle] = (r.get("n_orders") or "",
                             nb(r.get("total_qty")),
                             nb(r.get("qty_within_5pct")))

    for f in sorted(glob.glob("data/tape/*/*.csv.gz")):
        with io.StringIO(texte_gz(f)) as fh:
            for r in lignes(fh):
                if r.get("evt") != "C" or r.get("reprise") == "1":
                    continue
                d, p = nb(r.get("delta")), nb(r.get("price"))
                if not d or d <= 0 or p is None:
                    continue
                q = int(r.get("quality") or 0)
                cle = (r["ts"][:13], int(r["kind"]), q)
                vendu[cle] += d
                e = volp[(cle[0], cle[1], cle[2], round(p, 4))]
                e[0] += d
                e[1] += 1

    par_mois_h, par_mois_v = defaultdict(list), defaultdict(list)
    for cle in sorted(set(prix) | set(etat) | set(vendu)):
        h, k, q = cle
        p = prix.get(cle) or []
        n, qte, prof = etat.get(cle, ("", None, None))
        par_mois_h[h[:7]].append([
            h, k, q,
            p[0] if p else "", max(p) if p else "", min(p) if p else "",
            p[-1] if p else "", n,
            round(qte) if qte is not None else "",
            round(prof) if prof is not None else "",
            round(vendu.get(cle, 0)), ""])
    for (h, k, q, p), (v, n) in sorted(volp.items()):
        par_mois_v[(k, h[:7])].append([h, k, q, p, round(v), n])

    ecrire("data/horaire", EN_HORAIRE, par_mois_h)
    for (k, mois), l in par_mois_v.items():
        os.makedirs(f"data/volume/{k}", exist_ok=True)
        with open(f"data/volume/{k}/{mois}.csv", "w", newline="") as fh:
            w = csv.writer(fh); w.writerow(EN_VOLUME); w.writerows(l)
    print(f"  data/volume : {len(par_mois_v)} fichiers par ressource")

    for d in ("data/ticker", "data/book", "data/tape", "data/hourly",
              "data/flow"):
        if os.path.isdir(d):
            shutil.rmtree(d)
            print(f"  {d} efface")


def ecrire(dossier, entete, par_mois):
    os.makedirs(dossier, exist_ok=True)
    for mois, l in par_mois.items():
        with open(f"{dossier}/{mois}.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(entete)
            w.writerows(l)
        print(f"  {dossier}/{mois}.csv : {len(l)} lignes")


if __name__ == "__main__":
    main()
