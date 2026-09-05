#!/usr/bin/env python3
"""Collecteur de marche Sim Companies.

PRINCIPE
--------
Le programme ne se souvient de rien. Sa memoire, c'est le fichier.

A chaque demarrage il lit le carnet vivant sur la branche `live`, ainsi que
le CURSEUR qui dit ou le run precedent s'est arrete. Il lit alors LOT
ressources (dix par defaut) en repartant juste apres le curseur, compare ce
qu'il voit a ce que le fichier disait, en deduit ce qui s'est vendu,
reecrit le fichier et le curseur — puis il s'arrete.

UN RUN, UN CYCLE
----------------
run   : une execution du programme. Il lit LOT carnets, enregistre, sort.
        Une ou deux minutes, pas six heures.
cycle : le tour complet des 142 ressources, soit 15 runs enchaines a LOT=10.
        Arrive au bout de la liste, le curseur revient au debut tout seul.

Le programme ne sait pas qu'il fait partie d'une chaine, et n'a pas besoin
de le savoir : tout ce qui doit survivre est dans le fichier. Si un run
meurt, celui qui le remplace repart du dernier curseur enregistre.

DEUX BRANCHES
-------------
`live`  : reecrite par-dessus elle-meme a chaque envoi (un seul exemplaire
          conserve, zero historique)
            ordres.csv         les offres en cours, avec leur variation
            heure_horaire.csv  l'heure en cours, en construction
            heure_volume.csv   les volumes par prix de l'heure en cours
            curseur.txt        le dernier produit lu : ou reprendre

`main`  : ne recoit qu'une heure TERMINEE, ajoutee une fois pour toutes
            data/horaire/AAAA-MM.csv        toutes ressources, pour les vues
                                            d'ensemble
            data/volume/<ressource>/AAAA-MM.csv   range par ressource : le
                                            tableau de bord ne charge que le
                                            produit qu'on regarde

TROIS COLONNES, TROIS DEGRES DE CERTITUDE
-----------------------------------------
vendu   : une offre a PERDU une partie de sa quantite. Le jeu ne permet pas
          a un vendeur de reduire son offre : c'est donc une vente, mesuree,
          sans discussion.
disparu : l'offre entiere a disparu ALORS QU'ELLE ETAIT LA MOINS CHERE.
          Personne n'aurait achete ailleurs, donc c'est probablement une
          vente — mais ce peut aussi etre une annulation ou une remise en
          vente a un autre prix. C'est une DEDUCTION, pas une mesure.
repose  : l'offre a disparu ALORS QU'ELLE NE POUVAIT PAS ETRE ACHETEE (une
          moins chere est restee intacte), et le MEME vendeur a repose au
          meme instant sur la meme qualite : il a change son prix.

          L'ordre des regles compte. Le front passe TOUJOURS en premier :
          il dit ce qui pouvait physiquement etre achete. La remise en vente
          ne fait que departager ce qui, de toute facon, ne pouvait pas
          l'etre. Ainsi le filtre ne peut jamais effacer une vente reelle —
          un vendeur qui ecoule son offre la moins chere puis en repose une
          neuve reste compte comme une vente.
retire  : l'offre entiere a disparu alors qu'une offre MOINS CHERE est
          restee intacte. Un acheteur aurait pris la moins chere d'abord :
          celle-ci n'a donc pas ete vendue, son vendeur l'a retiree.

COMMENT ON SEPARE VENTE ET RETRAIT
----------------------------------
Un achat consomme le carnet par le bas, en bloc continu : la moins chere
d'abord, puis la suivante, jusqu'a ce que la quantite voulue soit atteinte.
Les offres du milieu disparaissent donc entierement sans avoir jamais ete
"la moins chere" au moment ou on les regarde.

On repere donc le FRONT : le prix de la premiere offre restee INTACTE (encore
la, meme quantite). Tout ce qui a ete consomme en dessous de ce front fait
partie du meme achat — c'est vendu. Ce qui a disparu au-dessus du front ne
peut pas avoir ete achete, puisqu'il restait moins cher juste a cote : c'est
un retrait.

On garde les deux premieres separees parce qu'elles ne valent pas la meme
chose : sur la citrouille, les baisses partielles totalisent quelques
milliers d'unites par heure, les disparitions des centaines de milliers.
Melangees, la deduction ecraserait la mesure.
"""
import csv, functools, glob, io, json, os, subprocess, sys, threading, time
import urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

print = functools.partial(print, flush=True)

REALM = 0
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
BOOK = "https://www.simcompanies.com/api/v3/market/all/%d/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"

# --------------------------------------------------------------- le lot
#
# Un RUN ne fait plus tourner une boucle de six heures : il lit LOT carnets,
# enregistre, et s'arrete. Le run suivant reprend la ou celui-ci s'est
# arrete, grace au curseur ecrit sur la branche live. Quinze runs de dix
# produits font le tour des 142 : c'est un CYCLE.
LOT = int(os.environ.get("LOT", "10"))                   # carnets par run
CURSEUR = "curseur.txt"                                  # memoire du rang
TEMOIN = "collecte_ok"                                   # preuve de fin propre

# Garde-fou : si quelque chose se bloque (reseau qui pend, quota qui
# s'effondre), le run s'arrete de lui-meme au lieu de manger le timeout du
# job. Ce n'est PLUS la duree voulue, seulement un plafond.
DUREE = int(os.environ.get("DUREE_MIN", "20")) * 60
BRANCHE = os.environ.get("GITHUB_REF_NAME", "main")

# ---------------------------------------------------------- le bon rythme
#
# MESURE, sur le runner, avec des ressources jamais reutilisees (donc sans
# cache pour fausser le compte) :
#
#   - dix lectures acceptees par minute, pas une de plus
#   - au-dela, le serveur refuse ; les refus sont gratuits mais inutiles
#   - 1096 requetes forcees en 7 minutes n'ont jamais donne plus de 10,7
#     lectures par minute
#
# Il n'y a donc rien a gagner a pousser : la seule chose intelligente est de
# ne JAMAIS depasser, et de bien choisir ce qu'on demande. D'ou la fenetre
# glissante ci-dessous, qui rend un refus structurellement impossible.
#
# Et surtout : market-ticker rend les 142 prix en UNE requete. Un dixieme du
# budget suffit donc a tenir tous les prix a jour a la minute ; le reste va
# aux carnets detailles, qui seuls donnent la qualite, la profondeur et les
# volumes.
QUOTA = [int(os.environ.get("QUOTA", "10"))]      # lectures autorisees...
FENETRE = float(os.environ.get("FENETRE", "60"))  # ...par tranche de X secondes
QUOTA_MIN = int(os.environ.get("QUOTA_MIN", "5"))
MARGE = float(os.environ.get("MARGE", "0.3"))     # petit coussin de securite
TICKER_SEC = float(os.environ.get("TICKER_SEC", "60"))   # un ticker par minute

# Le tour est desormais strictement sequentiel : chaque produit est lu une
# fois par cycle, ni plus ni moins. SUIVIS reste le seul privilege : ces
# produits-la sont relus a CHAQUE run, en plus du lot du moment. Deux ou
# trois suffisent ; au-dela, ils mangeraient tout le budget du run.
SUIVIS = {int(x) for x in os.environ.get("SUIVIS", "").replace(";", ",")
          .split(",") if x.strip().isdigit()}

N429 = [0]
AVANCEE = {"lues": 0, "total": 0, "tickers": 0, "journal": 0.0, "refus": 0}

CHRONO = {"attente": 0.0, "reseau": 0.0, "traitement": 0.0, "envoi": 0.0}


def chrono_zero():
    for c in CHRONO:
        CHRONO[c] = 0.0


def chrono_texte(total):
    p = lambda v: f"{v/60:.1f} min ({v/total*100:.0f} %)" if total > 0 else "—"
    return (f"attente quota {p(CHRONO['attente'])} · reseau {p(CHRONO['reseau'])}"
            f" · traitement {p(CHRONO['traitement'])} · envois {p(CHRONO['envoi'])}")


_porte = threading.Lock()
_recentes = []          # instants des requetes envoyees, fenetre glissante


def attendre_son_tour():
    """Ne laisse JAMAIS partir plus de QUOTA requetes par FENETRE secondes.
    On ne subit donc pas la limite : on vit dedans. C'est la difference entre
    demander la permission et se la faire refuser."""
    while True:
        with _porte:
            maintenant = time.time()
            while _recentes and _recentes[0] <= maintenant - FENETRE:
                _recentes.pop(0)
            if len(_recentes) < QUOTA[0]:
                _recentes.append(maintenant)
                return
            # la place se libere quand la plus ancienne sort de la fenetre
            attente = _recentes[0] + FENETRE - maintenant + MARGE
        CHRONO["attente"] += attente
        time.sleep(max(0.05, attente))


def rythme_refus():
    """Un refus veut dire que notre quota est trop genereux : on le baisse
    d'un cran, definitivement pour ce run. Il n'y a pas de penalite a subir,
    juste une estimation a corriger."""
    N429[0] += 1
    AVANCEE["refus"] += 1
    if QUOTA[0] > QUOTA_MIN:
        QUOTA[0] -= 1
        print(f"  refus : quota ramene a {QUOTA[0]} lectures par "
              f"{FENETRE:.0f} s")


def rythme_succes():
    pass


def journal_avancee(force=False):
    if not force and time.time() - AVANCEE["journal"] < 60:
        return
    AVANCEE["journal"] = time.time()
    if not AVANCEE["total"]:
        return
    print(f"  {AVANCEE['tickers']} releve(s) de prix (142 produits chacun)"
          f" · {AVANCEE['lues']} carnet(s) detaille(s)"
          f" · quota {QUOTA[0]}/{FENETRE:.0f} s"
          + (f" · {AVANCEE['refus']} refus" if AVANCEE["refus"] else ""))


# ---------------------------------------------------------------- reseau

def fetch(url, tries=2, cadence=False):
    # Une meme ressource peut etre refusee ses 3 tentatives d'affilee. Si
    # chacune faisait monter le rythme, UNE ressource indisponible suffirait a
    # doubler le delai de toutes les autres. On ne compte donc que le premier
    # refus de cette ressource-ci ; les suivants sont le meme incident.
    signale = False
    for i in range(tries):
        if cadence:
            attendre_son_tour()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                       "Accept": "application/json"})
            t_res = time.time()
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode())
            CHRONO["reseau"] += time.time() - t_res
            rythme_succes()
            return d
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if not signale:
                    rythme_refus()
                    signale = True
                else:
                    N429[0] += 1
                # (ici se trouvait un `_prochain[0] = ...` qui n'existait
                # nulle part : le premier 429 porteur d'un Retry-After faisait
                # planter le collecteur. Le quota glissant suffit a nous tenir
                # sous la limite, on se contente donc de reessayer.)
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


def iso(x):
    """Normalise la date de mise en vente donnee par le jeu
    (2026-09-04T13:15:47.515470+00:00) au format court."""
    if not x or len(x) < 19:
        return ""
    return x[:19] + "Z"


# ------------------------------------------------------------------- git

def git(*a, entree=None):
    return subprocess.run(["git", *a], input=entree,
                          capture_output=True, text=True)


EN_ORDRES = ["kind", "quality", "order_id", "seller_id", "price", "qty",
             "delta", "depuis", "passage"]
#            depuis  : date de mise en vente donnee par le jeu
#            passage : dernier tour ou le collecteur a VU cette offre. Si elle
#                      n'a pas ete revue depuis longtemps, c'est que sa
#                      ressource n'est plus jointe : le tableau de bord doit
#                      pouvoir le dire au lieu d'afficher un carnet perime.
EN_HORAIRE = ["heure", "kind", "quality", "ouverture", "haut", "bas",
              "cloture", "n_ordres", "qte_totale", "profondeur_5pct",
              "vendu", "disparu", "repose", "retire", "pose"]
# pose : quantite apparue pendant l'heure (offres neuves + quantite ajoutee a
#        une offre existante). C'est elle qui permet de fermer l'equation :
#
#   stock(fin) = stock(debut) + pose - (vendu + disparu + repose + retire)
#
#        Si le compte ne tombe pas juste, la difference est du mouvement qu'on
#        n'a PAS vu — une offre posee puis entamee entre deux passages. On ne
#        peut pas l'observer, mais on peut desormais le CHIFFRER.
EN_VOLUME = ["heure", "kind", "quality", "prix", "vendu", "disparu", "n_evt"]

# Le flux de prix, alimente par market-ticker : les 142 produits en UNE
# requete. C'est lui qui porte l'historique de prix, a la minute, pour tout le
# marche. Les carnets detailles restent la source de la qualite, de la
# profondeur et des volumes — mais ils n'ont plus a porter les prix.
EN_PRIX = ["heure", "kind", "ouverture", "haut", "bas", "cloture", "n_releves"]
EN_INSTANT = ["kind", "prix", "sens", "passage"]



def en_csv(entete, lignes):
    s = io.StringIO()
    w = csv.writer(s, lineterminator="\n")
    w.writerow(entete)
    w.writerows(lignes)
    return s.getvalue()


def lire_csv(texte, entete):
    """Lit un CSV en se fiant aux NOMS des colonnes, jamais a leur ordre.

    Avant, un en-tete different faisait tout jeter — donc le jour ou on
    ajoutait une colonne, le programme perdait sa memoire entiere au
    redemarrage et repartait de zero. Maintenant il retrouve chaque colonne
    par son nom ; celles qui n'existaient pas restent vides."""
    if not texte:
        return []
    lignes = texte.strip().split("\n")
    if len(lignes) < 2:
        return []
    vieux = [c.strip() for c in lignes[0].strip("\r").split(",")]
    out = []
    for l in csv.reader(lignes[1:]):
        if len(l) != len(vieux):
            continue
        d = dict(zip(vieux, l))
        out.append([d.get(c, "") for c in entete])
    return out


# Comment deux enregistrements de la meme heure se combinent.
SOMME = {"vendu", "disparu", "repose", "retire", "pose", "n_evt",
         "n_releves"}
PLUS_HAUT = {"haut"}
PLUS_BAS = {"bas"}
PREMIER = {"ouverture"}          # garde la valeur la plus ancienne
CLES = {"heure", "kind", "quality", "prix"}


def combiner(entete, a, b):
    """a = ce qui etait deja enregistre, b = ce qu'on ajoute."""
    def nb(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    out = []
    for i, col in enumerate(entete):
        va, vb = a[i], b[i]
        if col in CLES:
            out.append(va)
        elif col in SOMME:
            out.append(round((nb(va) or 0) + (nb(vb) or 0)))
        elif col in PLUS_HAUT:
            xs = [x for x in (nb(va), nb(vb)) if x is not None]
            out.append(max(xs) if xs else "")
        elif col in PLUS_BAS:
            xs = [x for x in (nb(va), nb(vb)) if x is not None]
            out.append(min(xs) if xs else "")
        elif col in PREMIER:
            out.append(va if va != "" else vb)
        else:
            out.append(vb if vb != "" else va)     # etat le plus recent
    return out


def charger_live():
    """La memoire du programme : le carnet et l'heure en cours, tels que le
    programme precedent les a laisses."""
    r = git("fetch", "--depth=1", "--force", "origin", "live")
    if r.returncode != 0:
        print("  branche live absente — premier demarrage, on repart a neuf")
        return {}, {}, {}, {}, {}, None
    def lire(nom, entete):
        s = git("show", f"FETCH_HEAD:{nom}")
        return lire_csv(s.stdout, entete) if s.returncode == 0 else []

    ordres = {}
    for k, q, oid, sid, p, qt, dl, dep, psg in lire("ordres.csv", EN_ORDRES):
        ordres[oid] = {"kind": int(k), "q": int(q), "sid": sid, "p": float(p),
                       "qt": float(qt), "depuis": dep, "passage": psg,
                       "delta": dl}

    agg = {}
    for r_ in lire("heure_horaire.csv", EN_HORAIRE):
        h, k, q = r_[0], int(r_[1]), int(r_[2])
        agg[(h, k, q)] = {
            "o": flt(r_[3]), "h": flt(r_[4]), "b": flt(r_[5]), "c": flt(r_[6]),
            "n": int(r_[7] or 0), "qte": flt(r_[8]) or 0.0,
            "prof": flt(r_[9]), "vendu": flt(r_[10]) or 0.0,
            "disparu": flt(r_[11]) or 0.0, "repose": flt(r_[12]) or 0.0,
            "retire": flt(r_[13]) or 0.0, "pose": flt(r_[14]) or 0.0}

    volp = {}
    for h, k, q, p, v, d, n in lire("heure_volume.csv", EN_VOLUME):
        volp[(h, int(k), int(q), float(p))] = [float(v), float(d), int(n)]

    pxh = {}
    for h, k, o, ha, ba, c, n in lire("heure_prix.csv", EN_PRIX):
        pxh[(h, int(k))] = {"o": flt(o), "h": flt(ha), "b": flt(ba),
                            "c": flt(c), "n": int(n or 0)}

    instant = {}
    for k, p, sens, psg in lire("prix.csv", EN_INSTANT):
        instant[int(k)] = {"p": flt(p), "sens": sens, "passage": psg}

    # le curseur : le dernier produit lu par le run precedent. C'est tout ce
    # qu'il faut pour reprendre le tour exactement ou il en etait.
    dernier = None
    s = git("show", f"FETCH_HEAD:{CURSEUR}")
    if s.returncode == 0 and s.stdout.strip().lstrip("-").isdigit():
        dernier = int(s.stdout.strip())

    print(f"  memoire reprise : {len(ordres)} ordres, "
          f"{len(agg)} heures en cours, {len(volp)} paliers de prix, "
          f"{len(pxh)} heures de prix"
          + (f", curseur apres le produit {dernier}" if dernier is not None
             else ", pas de curseur (on repart du debut)"))
    return ordres, agg, volp, pxh, instant, dernier


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


def ajouter_main(chemin, entete, lignes, heures):
    """Ajoute des heures TERMINEES a un fichier d'historique.

    Si une heure y figure deja — parce qu'un autre passage l'avait deja
    ecrite en partie — les deux enregistrements se COMBINENT : les ventes
    s'additionnent, le plus haut et le plus bas gardent leurs extremes,
    l'ouverture reste la premiere connue. On ne remplace jamais : chaque
    processus ne voit qu'une partie des ressources avant de mourir, et
    ecraser reviendrait a jeter le travail du precedent."""
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    lignes = [[str(x) for x in l] for l in lignes]

    if not os.path.exists(chemin):
        with open(chemin, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(entete)
            w.writerows(lignes)
        return

    with open(chemin, newline="") as fh:
        anciennes = lire_csv(fh.read(), entete)
    with open(chemin, newline="") as fh:
        meme_entete = next(csv.reader(fh), []) == entete

    idx = [i for i, c in enumerate(entete) if c in CLES]
    cle = lambda l: tuple(l[i] for i in idx)
    touchees = {c for c in (cle(l) for l in anciennes) if c[0] in heures}
    a_combiner = touchees & {cle(l) for l in lignes}

    if meme_entete and not a_combiner:
        with open(chemin, "a", newline="") as fh:
            csv.writer(fh).writerows(lignes)
        return

    if not meme_entete:
        print(f"  {chemin} : en-tete mis a jour, fichier reecrit")
    if a_combiner:
        print(f"  {chemin} : {len(a_combiner)} ligne(s) completee(s) "
              f"au lieu d'etre ecrasee(s)")

    # les doublons deja presents dans le fichier se combinent eux aussi :
    # une heure ecrite deux fois par le passe redevient une seule ligne juste
    table = {}
    for l in anciennes:
        k = cle(l)
        table[k] = combiner(entete, table[k], l) if k in table else l
    for l in lignes:
        k = cle(l)
        table[k] = combiner(entete, table[k], l) if k in table else l

    with open(chemin, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(entete)
        w.writerows(sorted(table.values(),
                           key=lambda x: (x[0], int(x[1]), int(x[2]))))


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

# ressource -> numeros d'ordres, tenu a jour en meme temps que le carnet
PAR_KIND = {}


def reindexer(ordres):
    PAR_KIND.clear()
    for oid, v in ordres.items():
        PAR_KIND.setdefault(v["kind"], set()).add(oid)


def traiter(kind, book, ordres, agg, volp, ts):
    """Compare le carnet recu a ce qu'on avait, et en tire les ventes."""
    h = heure_de(ts)
    par_q = {}
    for x in book:
        par_q.setdefault(int(x.get("quality", 0)), []).append(x)

    neuf = {str(x["id"]): x for x in book}
    # On ne balaie plus les 11 000 ordres du carnet entier a chaque ressource :
    # un index par ressource est tenu a jour au fil de l'eau. Sur un tour, ca
    # remplace 1,6 million de comparaisons par 142 lectures d'index.
    anciens_par_q = {}
    for oid in list(PAR_KIND.get(kind, ())):
        v = ordres.get(oid)
        if v is None:
            PAR_KIND[kind].discard(oid)
            continue
        anciens_par_q.setdefault(v["q"], []).append(oid)

    for q in set(par_q) | set(anciens_par_q):
        offres = sorted(par_q.get(q, []), key=lambda x: x["price"])

        if offres:
            meilleur = offres[0]["price"]
            total = sum(o["quantity"] for o in offres)
            prof = sum(o["quantity"] for o in offres
                       if o["price"] <= meilleur * 1.05)
            a = agg.get((h, kind, q))
            if a is None:
                a = agg[(h, kind, q)] = {"o": meilleur, "h": meilleur,
                                         "b": meilleur, "c": meilleur, "n": 0,
                                         "qte": 0.0, "prof": None, "vendu": 0.0,
                                         "disparu": 0.0, "repose": 0.0,
                                         "retire": 0.0, "pose": 0.0}
            a["h"] = max(a["h"], meilleur)
            a["b"] = min(a["b"], meilleur)
            a["c"] = meilleur
            a["n"] = len(offres)
            a["qte"] = total
            a["prof"] = prof
        elif (h, kind, q) not in agg:
            continue          # rien en vente et rien a comparer

        # --- le front : la premiere offre restee intacte ---------------
        front = float("inf")
        for oid in anciens_par_q.get(q, []):
            av = ordres[oid]
            x = neuf.get(oid)
            if x is not None and float(x["quantity"]) == av["qt"]:
                front = min(front, av["p"])

        # --- les offres que ce vendeur vient de reposer ----------------
        # Un vendeur ne peut pas modifier le prix d'une offre : il la retire
        # et en repose une autre, avec un nouveau numero. Vu du carnet, c'est
        # une disparition suivie d'une apparition. Si le meme vendeur
        # reapparait au meme instant sur la meme qualite, c'est presque
        # surement ca — pas une vente.
        # (avant : set(ordres) etait reconstruit ICI, donc pour CHAQUE qualite
        # de CHAQUE ressource — quinze millions d'insertions par tour pour rien)
        neufs_par_vendeur = {}
        for x in offres:
            if str(x["id"]) in ordres:
                continue
            v = str((x.get("seller") or {}).get("id", ""))
            if v:
                neufs_par_vendeur.setdefault(v, []).append(float(x["quantity"]))

        # --- ce qui a bouge -------------------------------------------
        for oid in anciens_par_q.get(q, []):
            av = ordres[oid]
            x = neuf.get(oid)
            if x is None:
                # offre entierement disparue
                ordres.pop(oid, None)
                PAR_KIND.get(kind, set()).discard(oid)
                if av["p"] <= front + 1e-9:
                    # elle etait dans le bloc balaye par un achat : vendue,
                    # meme si son vendeur a repose juste apres (il a ecoule
                    # son stock puis remis en vente ce qu'il vient de produire)
                    vendre(av["qt"], av["p"], kind, q, h, agg, volp, certain=False)
                    continue
                # au-dessus du front : aucun acheteur n'aurait pu la prendre.
                # Reste a savoir si son vendeur l'a simplement reprisee.
                b = agg.get((h, kind, q))
                if b is None:
                    continue
                reposees = neufs_par_vendeur.get(av["sid"])
                if reposees:
                    reposees.pop()
                    b["repose"] += av["qt"]
                else:
                    b["retire"] += av["qt"]
                continue
            qt = float(x["quantity"])
            delta = av["qt"] - qt
            if delta > 0:
                # un vendeur ne peut pas reduire son offre : c'est une vente
                vendre(delta, av["p"], kind, q, h, agg, volp, certain=True)
            elif delta < 0:
                b = agg.get((h, kind, q))          # le vendeur a rajoute
                if b is not None:
                    b["pose"] += -delta
            av["passage"] = ts
            av["delta"] = -delta if delta else ""
            av["qt"] = qt
            av["p"] = float(x["price"])

        # --- les offres qu'on ne connaissait pas -----------------------
        for x in offres:
            oid = str(x["id"])
            if oid in ordres:
                continue
            b = agg.get((h, kind, q))
            if b is not None:
                b["pose"] += float(x["quantity"])
            PAR_KIND.setdefault(kind, set()).add(oid)
            ordres[oid] = {
                "kind": kind, "q": q,
                "sid": str((x.get("seller") or {}).get("id", "")),
                "p": float(x["price"]), "qt": float(x["quantity"]),
                # la vraie date de mise en vente, donnee par le jeu
                "depuis": iso(x.get("posted")) or ts, "passage": ts, "delta": ""}


def traiter_ticker(tk, pxh, instant, ts):
    """Une requete, 142 prix. On en tire l'ouverture, le haut, le bas et la
    cloture de l'heure pour chaque produit — et l'etat du marche a l'instant."""
    h = heure_de(ts)
    for e in tk:
        k = int(e["kind"])
        p = flt(e.get("price"))
        if p is None:
            continue
        a = pxh.get((h, k))
        if a is None:
            a = pxh[(h, k)] = {"o": p, "h": p, "b": p, "c": p, "n": 0}
        a["h"] = max(a["h"], p)
        a["b"] = min(a["b"], p)
        a["c"] = p
        a["n"] += 1
        instant[k] = {"p": p, "sens": "hausse" if e.get("is_up") else "baisse",
                      "passage": ts}


def lignes_prix(pxh, heures=None):
    out = []
    for (h, k), a in sorted(pxh.items()):
        if heures is not None and h not in heures:
            continue
        out.append([h, k, a["o"], a["h"], a["b"], a["c"], a["n"]])
    return out


def lignes_instant(instant):
    return [[k, v["p"], v["sens"], v["passage"]]
            for k, v in sorted(instant.items())]


def vendre(qte, prix, kind, q, h, agg, volp, certain=True):
    """certain=True : baisse partielle, c'est une vente mesuree.
       certain=False : offre disparue, c'est une vente deduite."""
    a = agg.get((h, kind, q))
    if a is None:
        return
    a["vendu" if certain else "disparu"] += qte
    e = volp.setdefault((h, kind, q, round(prix, 4)), [0.0, 0.0, 0])
    e[0 if certain else 1] += qte
    e[2] += 1


def lignes_horaire(agg, heures=None):
    out = []
    for (h, k, q), a in sorted(agg.items()):
        if heures is not None and h not in heures:
            continue
        out.append([h, k, q, a["o"], a["h"], a["b"], a["c"], a["n"],
                    round(a["qte"]), round(a["prof"]) if a["prof"] is not None else "",
                    round(a["vendu"]), round(a["disparu"]), round(a["repose"]),
                    round(a["retire"]), round(a["pose"])])
    return out


def lignes_volume(volp, heures=None):
    out = []
    for (h, k, q, p), (v, d, n) in sorted(volp.items()):
        if heures is not None and h not in heures:
            continue
        out.append([h, k, q, p, round(v), round(d), n])
    return out


def lignes_ordres(ordres):
    out = []
    for oid, v in ordres.items():
        out.append([v["kind"], v["q"], oid, v["sid"], v["p"], round(v["qt"]),
                    v.get("delta", ""), v["depuis"], v["passage"]])
    # tri stable : deux versions successives du fichier se ressemblent au
    # maximum, ce qui garde l'envoi leger
    out.sort(key=lambda r: (r[0], r[1], r[4], r[2]))
    return out


# ------------------------------------------------------------------ boucle

def lot_a_lire(kinds, dernier):
    """Les LOT produits qui viennent APRES `dernier`, en refermant la boucle.

    On memorise un NUMERO de produit, pas un rang : si le jeu ajoute ou
    retire une ressource entre deux runs, le tour ne se decale pas et aucun
    produit ne se fait sauter."""
    if not kinds:
        return []
    if dernier is None:
        i = 0
    elif dernier in kinds:
        i = (kinds.index(dernier) + 1) % len(kinds)
    else:
        # le produit memorise n'existe plus : on reprend au suivant dans l'ordre
        i = sum(1 for k in kinds if k <= dernier) % len(kinds)
    return [kinds[(i + j) % len(kinds)] for j in range(min(LOT, len(kinds)))]


def main():
    debut = time.time()
    ordres, agg, volp, pxh, instant, dernier = charger_live()
    reindexer(ordres)

    tk = fetch(TICKER, tries=3, cadence=True)
    kinds = sorted({int(r["kind"]) for r in tk}) if tk else list(range(1, 156))
    if tk:
        traiter_ticker(tk, pxh, instant, stamp())
        AVANCEE["tickers"] += 1

    lot = lot_a_lire(kinds, dernier)
    # les suivis passent a chaque run, en plus du lot du moment
    sup = [k for k in sorted(SUIVIS) if k in kinds and k not in lot]
    a_lire = sup + lot
    AVANCEE["total"] = len(a_lire)

    runs_par_cycle = -(-len(kinds) // LOT) if LOT else 1
    print(f"{len(kinds)} produits au total · lot de {LOT} par run, "
          f"soit {runs_par_cycle} runs pour un cycle complet")
    print(f"  ce run lit : {', '.join(map(str, lot))}"
          + (f"  (+ suivis : {', '.join(map(str, sup))})" if sup else ""))
    print(f"  quota : {QUOTA[0]} requetes par {FENETRE:.0f} s")

    for k in a_lire:
        if time.time() - debut > DUREE:
            print("  ! plafond de duree atteint, on ferme ici")
            break
        ts = stamp()
        book = fetch(BOOK % (REALM, k), tries=2, cadence=True)
        if book:
            t0 = time.time()
            traiter(k, book, ordres, agg, volp, ts)
            CHRONO["traitement"] += time.time() - t0
            AVANCEE["lues"] += 1
        else:
            # on n'insiste pas : le curseur avance quand meme, ce produit
            # sera relu au prochain cycle. Bloquer le tour sur une ressource
            # indisponible affamerait les 141 autres.
            print(f"  carnet {k} indisponible, passe au suivant")

    # Un second releve de prix si le run a dure plus d'une minute : deux
    # points valent mieux qu'un pour l'heure en cours, et ca ne coute qu'une
    # requete sur les douze du run.
    if time.time() - debut >= TICKER_SEC:
        tkf = fetch(TICKER, tries=1, cadence=True)
        if tkf:
            traiter_ticker(tkf, pxh, instant, stamp())
            AVANCEE["tickers"] += 1

    # Le curseur avance au dernier produit DU LOT, lu ou non. Les suivis ne
    # comptent pas : ils sont hors tour.
    nouveau = lot[-1] if lot else dernier

    t0 = time.time()
    fermer_et_envoyer(ordres, agg, volp, pxh, instant, nouveau, final=True)
    CHRONO["envoi"] += time.time() - t0

    ecoule = time.time() - debut
    print(f"fin de run en {ecoule:.0f} s : {AVANCEE['lues']}/{len(a_lire)} "
          f"carnets lus, {AVANCEE['tickers']} releves de prix, "
          f"{N429[0]} refus · curseur -> {nouveau}")
    print("  " + chrono_texte(ecoule))

    # Le temoin : sa presence dit au workflow que ce run est alle au bout.
    # Sans lui, pas de relance — c'est ce qui empeche une boucle de
    # plantages instantanes de tourner a vide.
    with open(TEMOIN, "w") as fh:
        fh.write(stamp() + "\n")

def fermer_et_envoyer(ordres, agg, volp, pxh, instant, dernier_kind=None,
                      final=False):
    """Les heures terminees partent dans l'historique ; l'heure en cours, le
    carnet et les prix vont sur la branche live."""
    heures = {h for h, _, _ in agg} | {h for h, _ in pxh}
    en_cours = max(heures, default=None)
    finies = sorted(heures - {en_cours}) if en_cours else []

    if finies:
        for h in finies:
            mois = h[:7]
            ajouter_main(f"data/horaire/{mois}.csv", EN_HORAIRE,
                         lignes_horaire(agg, {h}), {h})
            par_res = {}
            for l in lignes_volume(volp, {h}):
                par_res.setdefault(l[1], []).append(l)
            for k, l in par_res.items():
                ajouter_main(f"data/volume/{k}/{mois}.csv", EN_VOLUME, l, {h})
            lp = lignes_prix(pxh, {h})
            if lp:
                ajouter_main(f"data/prix/{mois}.csv", EN_PRIX, lp, {h})
        for cle in [c for c in pxh if c[0] in finies]:
            del pxh[cle]
        for cle in [c for c in agg if c[0] in finies]:
            del agg[cle]
        for cle in [c for c in volp if c[0] in finies]:
            del volp[cle]
        pousser_main("heures " + ", ".join(finies))
        print(f"  {len(finies)} heure(s) archivee(s) : {', '.join(finies)}")

    fichiers = {
        "ordres.csv": en_csv(EN_ORDRES, lignes_ordres(ordres)),
        "heure_horaire.csv": en_csv(EN_HORAIRE, lignes_horaire(agg)),
        "heure_volume.csv": en_csv(EN_VOLUME, lignes_volume(volp)),
        "heure_prix.csv": en_csv(EN_PRIX, lignes_prix(pxh)),
        "prix.csv": en_csv(EN_INSTANT, lignes_instant(instant)),
    }
    # Le curseur voyage avec le carnet, dans le MEME commit : impossible
    # d'enregistrer les donnees sans enregistrer ou on en est, ou l'inverse.
    if dernier_kind is not None:
        fichiers[CURSEUR] = f"{dernier_kind}\n"
    ok = pousser_live(fichiers)
    if ok:
        print(f"  envoye : {len(instant)} prix a la minute, "
              f"{len(ordres)} ordres" +
              (" — dernier envoi" if final else ""))
        journal_avancee(force=True)


if __name__ == "__main__":
    main()
