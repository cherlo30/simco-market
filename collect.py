#!/usr/bin/env python3
"""Collecteur de marche Sim Companies.

PRINCIPE
--------
Le programme ne se souvient de rien. Sa memoire, c'est le fichier.

A chaque demarrage il lit le carnet vivant sur la branche `live`, puis il
tourne en boucle : il interroge les 143 ressources une par une, compare ce
qu'il voit a ce que le fichier disait, en deduit ce qui s'est vendu, et
reecrit le fichier. Si le programme meurt, celui qui le remplace reprend
exactement ou il en etait. Aucun trou.

DEUX BRANCHES
-------------
`live`  : reecrite par-dessus elle-meme a chaque envoi (un seul exemplaire
          conserve, zero historique)
            ordres.csv         les offres en cours, avec leur variation
            heure_horaire.csv  l'heure en cours, en construction
            heure_volume.csv   les volumes par prix de l'heure en cours

`main`  : ne recoit qu'une heure TERMINEE, ajoutee une fois pour toutes
            data/horaire/AAAA-MM.csv        toutes ressources, pour les vues
                                            d'ensemble
            data/volume/<ressource>/AAAA-MM.csv   range par ressource : le
                                            tableau de bord ne charge que le
                                            produit qu'on regarde

VENDU OU RETIRE
---------------
Le jeu ne permet pas de reduire une offre en vente. Donc toute BAISSE
PARTIELLE est une vente, sans discussion possible.

Seule la disparition complete d'une offre est ambigue : vendue jusqu'au
dernier, ou retiree par son vendeur. On tranche par le prix — si l'offre
disparue etait au meilleur prix ou en dessous, personne n'aurait achete
ailleurs, c'est une VENTE ; s'il restait moins cher sur le marche, c'est un
RETRAIT.
"""
import csv, functools, io, json, os, subprocess, sys, time
import urllib.request, urllib.error
from datetime import datetime, timezone

print = functools.partial(print, flush=True)

REALM = 0
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
BOOK = "https://www.simcompanies.com/api/v3/market/all/%d/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"

DELAY = float(os.environ.get("DELAY", "1.3"))            # entre deux requetes
DUREE = int(os.environ.get("DUREE_MIN", "330")) * 60     # duree de vie
LIVE_SEC = int(os.environ.get("LIVE_SEC", "300"))        # envoi du carnet
BRANCHE = os.environ.get("GITHUB_REF_NAME", "main")

FREIN = [0.0]     # ralentissement automatique quand le jeu refuse
N429 = [0]


# ---------------------------------------------------------------- reseau

def fetch(url, tries=4):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode())
            FREIN[0] = max(0.0, FREIN[0] - 0.02)
            return d
        except urllib.error.HTTPError as e:
            if e.code == 429:
                N429[0] += 1
                FREIN[0] = min(4.0, FREIN[0] + 0.3)
                attente = 0.0
                try:
                    attente = float(e.headers.get("Retry-After") or 0)
                except Exception:
                    pass
                time.sleep(max(attente, 3.0 * (i + 1)))
                continue
            if i == tries - 1:
                print(f"  ! {url.rsplit('/',3)[-3:]} : {e}")
            else:
                time.sleep(1.5 * (i + 1))
        except Exception as e:
            if i == tries - 1:
                print(f"  ! {url.rsplit('/',3)[-3:]} : {e}")
            else:
                time.sleep(1.5 * (i + 1))
    return None


def stamp(dt=None):
    return (dt or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")


def heure_de(ts):
    return ts[:13]              # AAAA-MM-JJTHH


# ------------------------------------------------------------------- git

def git(*a, entree=None):
    return subprocess.run(["git", *a], input=entree,
                          capture_output=True, text=True)


EN_ORDRES = ["kind", "quality", "order_id", "seller_id", "price", "qty",
             "delta", "vu", "maj"]
EN_HORAIRE = ["heure", "kind", "quality", "ouverture", "haut", "bas",
              "cloture", "n_ordres", "qte_totale", "profondeur_5pct",
              "vendu", "retire"]
EN_VOLUME = ["heure", "kind", "quality", "prix", "vendu", "n_evt"]


def en_csv(entete, lignes):
    s = io.StringIO()
    w = csv.writer(s, lineterminator="\n")
    w.writerow(entete)
    w.writerows(lignes)
    return s.getvalue()


def lire_csv(texte, entete):
    """Lit un CSV en ignorant les lignes qui n'ont pas le bon nombre de
    colonnes ou dont l'en-tete a change."""
    if not texte:
        return []
    lignes = texte.strip().split("\n")
    if not lignes or lignes[0].strip("\r") != ",".join(entete):
        return []
    out = []
    for l in csv.reader(lignes[1:]):
        if len(l) == len(entete):
            out.append(l)
    return out


def charger_live():
    """La memoire du programme : le carnet et l'heure en cours, tels que le
    programme precedent les a laisses."""
    r = git("fetch", "--depth=1", "--force", "origin", "live")
    if r.returncode != 0:
        print("  branche live absente — premier demarrage, on repart a neuf")
        return {}, {}, {}
    def lire(nom, entete):
        s = git("show", f"FETCH_HEAD:{nom}")
        return lire_csv(s.stdout, entete) if s.returncode == 0 else []

    ordres = {}
    for k, q, oid, sid, p, qt, dl, vu, maj in lire("ordres.csv", EN_ORDRES):
        ordres[oid] = {"kind": int(k), "q": int(q), "sid": sid,
                       "p": float(p), "qt": float(qt), "vu": vu, "maj": maj}

    agg = {}
    for r_ in lire("heure_horaire.csv", EN_HORAIRE):
        h, k, q = r_[0], int(r_[1]), int(r_[2])
        agg[(h, k, q)] = {
            "o": flt(r_[3]), "h": flt(r_[4]), "b": flt(r_[5]), "c": flt(r_[6]),
            "n": int(r_[7] or 0), "qte": flt(r_[8]) or 0.0,
            "prof": flt(r_[9]), "vendu": flt(r_[10]) or 0.0,
            "retire": flt(r_[11]) or 0.0}

    volp = {}
    for h, k, q, p, v, n in lire("heure_volume.csv", EN_VOLUME):
        volp[(h, int(k), int(q), float(p))] = [float(v), int(n)]

    print(f"  memoire reprise : {len(ordres)} ordres, "
          f"{len(agg)} heures en cours, {len(volp)} paliers de prix")
    return ordres, agg, volp


def flt(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def pousser_live(fichiers):
    """Reecrit la branche live par-dessus elle-meme : un commit sans parent,
    pousse en force. GitHub ne conserve donc jamais qu'un exemplaire."""
    entrees = []
    for nom, contenu in fichiers.items():
        h = git("hash-object", "-w", "--stdin", entree=contenu)
        if h.returncode != 0:
            print("  ! ecriture live impossible"); return False
        entrees.append(f"100644 blob {h.stdout.strip()}\t{nom}")
    t = git("mktree", entree="\n".join(entrees) + "\n")
    if t.returncode != 0:
        print("  ! arbre live impossible"); return False
    c = git("commit-tree", t.stdout.strip(), "-m", "carnet " + stamp())
    if c.returncode != 0:
        print("  ! commit live impossible"); return False
    p = git("push", "--force", "origin", f"{c.stdout.strip()}:refs/heads/live")
    if p.returncode != 0:
        print("  ! envoi live refuse : " + p.stderr.strip().splitlines()[-1:][0]
              if p.stderr.strip() else "  ! envoi live refuse")
        return False
    return True


def ajouter_main(chemin, entete, lignes):
    """Ajoute des heures TERMINEES a un fichier d'historique."""
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    neuf = not os.path.exists(chemin)
    if not neuf:
        with open(chemin) as fh:
            if fh.readline().strip() != ",".join(entete):
                # l'en-tete a change : on met l'ancien de cote
                n = 1
                while os.path.exists(f"{chemin[:-4]}.ancien{n}.csv"):
                    n += 1
                os.rename(chemin, f"{chemin[:-4]}.ancien{n}.csv")
                neuf = True
    with open(chemin, "a", newline="") as fh:
        w = csv.writer(fh)
        if neuf:
            w.writerow(entete)
        w.writerows(lignes)


def pousser_main(message):
    git("add", "data")
    if git("diff", "--staged", "--quiet").returncode == 0:
        return
    git("commit", "-m", message)
    for i in range(5):
        git("pull", "--rebase", "--autostash", "origin", BRANCHE)
        if git("push").returncode == 0:
            print("  historique enregistre")
            return
        time.sleep(5 + i * 5)
    print("  ! ECHEC de l'enregistrement de l'historique")


# ------------------------------------------------------------- traitement

def traiter(kind, book, ordres, agg, volp, ts):
    """Compare le carnet recu a ce qu'on avait, en tire les ventes."""
    h = heure_de(ts)
    par_q = {}
    for x in book:
        par_q.setdefault(int(x.get("quality", 0)), []).append(x)

    vus = set()
    for q, offres in par_q.items():
        offres.sort(key=lambda x: x["price"])
        meilleur = offres[0]["price"]
        total = sum(o["quantity"] for o in offres)
        prof = sum(o["quantity"] for o in offres
                   if o["price"] <= meilleur * 1.05)

        a = agg.get((h, kind, q))
        if a is None:
            a = agg[(h, kind, q)] = {"o": meilleur, "h": meilleur,
                                     "b": meilleur, "c": meilleur, "n": 0,
                                     "qte": 0.0, "prof": None,
                                     "vendu": 0.0, "retire": 0.0}
        a["h"] = max(a["h"], meilleur)
        a["b"] = min(a["b"], meilleur)
        a["c"] = meilleur
        a["n"] = len(offres)
        a["qte"] = total
        a["prof"] = prof

        for o in offres:
            oid = str(o["id"])
            vus.add(oid)
            av = ordres.get(oid)
            qt = float(o["quantity"])
            p = float(o["price"])
            if av is None:
                ordres[oid] = {"kind": kind, "q": q, "sid": str(
                    (o.get("seller") or {}).get("id", "")), "p": p, "qt": qt,
                    "vu": ts, "maj": ts, "delta": ""}
                continue
            delta = av["qt"] - qt
            if delta > 0:
                # une offre ne peut pas etre reduite par son vendeur :
                # une baisse partielle est toujours une vente
                vendre(delta, av["p"], kind, q, h, agg, volp)
            if delta != 0 or p != av["p"]:
                av["maj"] = ts
            av["delta"] = -delta if delta else ""
            av["qt"] = qt
            av["p"] = p
            av["q"] = q

    # les ordres de cette ressource qui ne sont plus la
    meilleurs = {q: min(o["price"] for o in offres)
                 for q, offres in par_q.items()}
    for oid in [o for o, v in ordres.items()
                if v["kind"] == kind and o not in vus]:
        v = ordres.pop(oid)
        mb = meilleurs.get(v["q"])
        classer_disparition(v["qt"], v["p"], mb, kind, v["q"], h, agg, volp)


def vendre(qte, prix, kind, q, h, agg, volp):
    """Une vente : on l'ajoute au total de l'heure ET au palier de prix."""
    a = agg.get((h, kind, q))
    if a is None:
        return
    a["vendu"] += qte
    e = volp.setdefault((h, kind, q, round(prix, 4)), [0.0, 0])
    e[0] += qte
    e[1] += 1


def classer_disparition(qte, prix, meilleur_restant, kind, q, h, agg, volp):
    """Une offre entiere a disparu. Au meilleur prix ou en dessous : plus
    personne n'aurait achete ailleurs, c'est une vente. Plus chere qu'une
    offre encore presente : le vendeur l'a retiree."""
    if meilleur_restant is None or prix <= meilleur_restant + 1e-9:
        vendre(qte, prix, kind, q, h, agg, volp)
    else:
        a = agg.get((h, kind, q))
        if a is not None:
            a["retire"] += qte


def lignes_horaire(agg, heures=None):
    out = []
    for (h, k, q), a in sorted(agg.items()):
        if heures is not None and h not in heures:
            continue
        out.append([h, k, q, a["o"], a["h"], a["b"], a["c"], a["n"],
                    round(a["qte"]), round(a["prof"]) if a["prof"] is not None else "",
                    round(a["vendu"]), round(a["retire"])])
    return out


def lignes_volume(volp, heures=None):
    out = []
    for (h, k, q, p), (v, n) in sorted(volp.items()):
        if heures is not None and h not in heures:
            continue
        out.append([h, k, q, p, round(v), n])
    return out


def lignes_ordres(ordres):
    out = []
    for oid, v in ordres.items():
        out.append([v["kind"], v["q"], oid, v["sid"], v["p"], round(v["qt"]),
                    v.get("delta", ""), v["vu"], v["maj"]])
    # tri stable : deux versions successives du fichier se ressemblent au
    # maximum, ce qui garde l'envoi leger
    out.sort(key=lambda r: (r[0], r[1], r[4], r[2]))
    return out


# ------------------------------------------------------------------ boucle

def main():
    debut = time.time()
    ordres, agg, volp = charger_live()

    tk = fetch(TICKER, tries=3)
    kinds = sorted({r["kind"] for r in tk}) if tk else list(range(1, 156))
    print(f"{len(kinds)} ressources suivies, un tour toutes les "
          f"~{len(kinds)*(DELAY+0.7)/60:.1f} min")

    dernier_live = 0.0
    tour = 0
    while time.time() - debut < DUREE:
        tour += 1
        t0 = time.time()
        for k in kinds:
            if time.time() - debut > DUREE:
                break
            book = fetch(BOOK % (REALM, k))
            if book:
                traiter(k, book, ordres, agg, volp, stamp())
            time.sleep(DELAY + FREIN[0])

            if time.time() - dernier_live > LIVE_SEC:
                dernier_live = time.time()
                fermer_et_envoyer(ordres, agg, volp)

        print(f"tour {tour} — {(time.time()-t0)/60:.1f} min, "
              f"{len(ordres)} ordres suivis, frein {FREIN[0]:.2f} s, "
              f"{N429[0]} refus 429")

    fermer_et_envoyer(ordres, agg, volp, final=True)
    print(f"{tour} tours effectues")


def fermer_et_envoyer(ordres, agg, volp, final=False):
    """Les heures terminees partent dans l'historique ; l'heure en cours et
    le carnet vont sur la branche live."""
    en_cours = max((h for h, _, _ in agg), default=None)
    finies = sorted({h for h, _, _ in agg if h != en_cours})

    if finies:
        for h in finies:
            mois = h[:7]
            ajouter_main(f"data/horaire/{mois}.csv", EN_HORAIRE,
                         lignes_horaire(agg, {h}))
            par_res = {}
            for l in lignes_volume(volp, {h}):
                par_res.setdefault(l[1], []).append(l)
            for k, l in par_res.items():
                ajouter_main(f"data/volume/{k}/{mois}.csv", EN_VOLUME, l)
        for cle in [c for c in agg if c[0] in finies]:
            del agg[cle]
        for cle in [c for c in volp if c[0] in finies]:
            del volp[cle]
        pousser_main("heures " + ", ".join(finies))
        print(f"  {len(finies)} heure(s) archivee(s) : {', '.join(finies)}")

    ok = pousser_live({
        "ordres.csv": en_csv(EN_ORDRES, lignes_ordres(ordres)),
        "heure_horaire.csv": en_csv(EN_HORAIRE, lignes_horaire(agg)),
        "heure_volume.csv": en_csv(EN_VOLUME, lignes_volume(volp)),
    })
    if ok:
        print(f"  carnet envoye ({len(ordres)} ordres)" +
              (" — dernier envoi" if final else ""))


if __name__ == "__main__":
    main()
