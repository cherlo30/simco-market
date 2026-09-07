#!/usr/bin/env python3
"""Collecteur de marche Sim Companies.

PRINCIPE
--------
Le programme ne se souvient de rien. Sa memoire, c'est le fichier.

A chaque demarrage il lit le carnet vivant sur la branche `live`, ainsi que
l'ETAT : pour chaque produit, quand on l'a lu pour la derniere fois et a
quel rythme il bouge. Il choisit alors les LOT produits dont il a
probablement rate le plus de choses, les lit, compare, en deduit ce qui
s'est vendu, reecrit tout — puis il s'arrete.

UN RUN, UN CYCLE
----------------
run   : une execution du programme. Il lit LOT carnets, enregistre, sort.
        Une quinzaine de secondes.
cycle : le temps qu'il faut pour que TOUS les produits aient ete lus au
        moins une fois. Ce n'est plus un tour de liste a longueur fixe :
        les produits actifs y passent plusieurs fois, les produits morts
        une seule.

QUI LIRE D'ABORD
----------------
    score = (evenements par minute + PLANCHER) x minutes depuis la LECTURE
            x BONUS_TICKER si le prix a change depuis cette lecture

L'age porte sur la derniere LECTURE, jamais sur le dernier mouvement. C'est
le point qui fait tout tenir : un produit tres actif ne peut pas se faire
oublier (son debit le ramene en tete en quelques secondes) et un produit
mort ne peut pas etre affame (son age grandit sans fin, et AGE_MAX finit de
toute facon par forcer sa lecture).

Trier par "a bouge il y a le plus longtemps" ferait exactement l'inverse :
le produit le plus liquide bouge sans arret, son dernier mouvement est donc
toujours recent, et il resterait eternellement en fin de file.

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
          moins chere est restee intacte), et le meme vendeur a remis en
          vente. Il faut TROIS preuves reunies, pas une :
            1. l'offre etait inachetable — au-dessus du front ;
            2. le vendeur a une offre neuve sur la meme qualite, posee
               APRES le dernier instant ou l'on a vu l'ancienne (la date de
               mise en vente vient du jeu, ce n'est pas une supposition) ;
            3. cette offre neuve est a un AUTRE prix — repositionner, c'est
               changer de prix ; sinon ce n'est pas un repositionnement.
          Et seule la quantite REVENUE compte comme repose : 15 M retires
          contre 10 M reposes, ce sont 10 M de repose et 5 M de retrait.

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
# Le detail : prix moyen et saturation par ressource, un seul appel pour
# les 60 produits vendables. Ces valeurs bougent au jour le jour, pas a la
# minute : une lecture par cycle suffit largement.
DETAIL_URL = "https://www.simcompanies.com/api/v4/%d/resources-retail-info/"
BOOK = "https://www.simcompanies.com/api/v3/market/all/%d/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"

# --------------------------------------------------------------- le lot
#
# Un RUN ne fait plus tourner une boucle de six heures : il lit LOT carnets,
# enregistre, et s'arrete. Le run suivant reprend la ou celui-ci s'est
# arrete, grace au curseur ecrit sur la branche live. Seize runs de neuf
# produits font le tour des 142 : c'est un CYCLE.
#
# POURQUOI NEUF ET PAS DIX
# ------------------------
# Le budget d'un run, c'est LOT carnets + 1 releve de prix. Tant que ce
# total tient dans le QUOTA (10 requetes par 60 s), les requetes partent
# TOUTES d'un coup : le run dure le temps du reseau, une quinzaine de
# secondes. Des qu'on depasse d'une seule requete, la fenetre glissante
# oblige a attendre qu'une place se libere — soit pres d'une minute pleine,
# facturee par GitHub, pour une seule lecture de plus. Mesure sur un vrai
# run a LOT=10 : 65 s dont 54 s d'attente pure, soit 87 % du temps a ne
# rien faire.
#
# Le plancher est ailleurs, et il est physique : 142 carnets a 10 requetes
# par minute font 14,2 min de cycle, qu'on les decoupe en 2 runs ou en 20.
# On ne peut donc pas aller plus vite — seulement arreter de payer de
# l'attente en plus du plancher.
LOT = int(os.environ.get("LOT", "9"))                    # carnets par run
CURSEUR = "curseur.txt"     # le rang, en clair, pour un oeil humain
ETAT = "etat.json"          # le meme rang + de quoi raconter le cycle
TEMOIN = "collecte_ok"      # preuve, pour le workflow, d'une fin propre
ETIQUETTE = "prochain.txt"  # le nom a donner au run suivant, dans Actions

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

# Le releve de prix ne sert a rien s'il est fait deux fois dans la meme
# demi-minute : les 142 prix ne bougent pas si vite. Le faire un run sur
# TICKER_RUNS libere une place de quota pour un carnet de plus, sans rien
# perdre de mesurable. A ~15 s par run, 2 donne un releve toutes les 30 s
# et 4 un releve par minute.
TICKER_RUNS = max(1, int(os.environ.get("TICKER_RUNS", "2")))

# ------------------------------------------------------- qui lire d'abord
#
# Le tour n'est plus sequentiel : on lit en priorite la ou il se passe des
# choses. Encore faut-il une regle qui ne laisse personne de cote.
#
# LE PIEGE, ET POURQUOI ON NE TRIE PAS PAR "A BOUGE IL Y A LONGTEMPS"
# Classer par anciennete du dernier MOUVEMENT affame exactement le produit
# le plus liquide : comme il bouge sans arret, son dernier mouvement est
# toujours recent, donc il reste eternellement en fin de file. C'est
# l'inverse du but recherche.
#
# LA REGLE : on classe par ce qu'on a probablement RATE depuis la derniere
# lecture.
#
#     score = (evenements par minute + plancher) x minutes depuis la lecture
#
# Le second facteur est un age de LECTURE, pas de mouvement : il ne cesse
# jamais de grandir, pour personne. Un produit tres actif remonte donc en
# tete au bout de quelques secondes ; un produit mort met des heures, mais
# il y arrive. Rien ne peut mourir de faim.
#
# On compte des EVENEMENTS (offres apparues, entamees, disparues) et non des
# unites : c'est sans dimension, donc l'electricite qui s'echange par
# millions ne noie pas les boules de Noel qui s'echangent par dizaines. Et
# c'est justement le nombre d'evenements entre deux lectures qui degrade la
# deduction du front — on asservit donc la cadence a ce qui abime la mesure.
PLANCHER = float(os.environ.get("PLANCHER", "0.05"))    # evt/min plancher
MEMOIRE = float(os.environ.get("MEMOIRE", "0.3"))       # poids du dernier releve
BONUS_TICKER = float(os.environ.get("BONUS_TICKER", "2"))

# DEUX ECHEANCES FERMES, qui passent devant le score.
#
# AGE_BOUGE : quand le ticker dit que le prix a change depuis notre derniere
# lecture, on SAIT qu'il s'est passe quelque chose qu'on n'a pas vu. Le
# bonus de score ne suffit pas : un produit endormi qui se reveille a un
# debit nul, donc un score faible, et il pourrait attendre cinq minutes
# derriere les habitues. On lui met donc une echeance : passe ce delai, il
# est lu, point.
#
# AGE_MAX : le filet pour tout le reste — un produit peut s'echanger sans
# que son prix bouge (carnet profond, ventes au meme prix), le ticker ne le
# verra jamais. Personne n'attend plus que ca.
# MESURE, sur 8 h de collecte : 76 % du volume et 69 % de la valeur
# s'echangent HORS qualite 0, sur 7,8 qualites actives par produit en
# moyenne. Or le ticker ne rend qu'UN prix par produit. Il est donc aveugle
# aux trois quarts de l'activite — ventes sur les autres qualites, ajouts
# qui ne changent pas le meilleur prix. AGE_MAX n'est pas un filet de
# securite lointain : c'est le principal moyen de voir le marche.
AGE_BOUGE = float(os.environ.get("AGE_BOUGE_MIN", "4")) * 60
AGE_MAX = float(os.environ.get("AGE_MAX_MIN", "5")) * 60

# SUIVIS reste le seul privilege absolu : ces produits sont relus a CHAQUE
# run, en plus du lot. Deux ou trois suffisent.
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


def slot_libre():
    """Reste-t-il une place dans la fenetre, la tout de suite ? Sert a decider
    si une requete facultative (le second releve de prix) est gratuite ou si
    elle couterait une minute d'attente. Dans le doute, on s'en passe."""
    with _porte:
        maintenant = time.time()
        while _recentes and _recentes[0] <= maintenant - FENETRE:
            _recentes.pop(0)
        return len(_recentes) < QUOTA[0]


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
        return {}, {}, {}, {}, {}, {}
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

    # L'etat : le curseur, plus ce qu'il faut pour raconter le cycle en
    # cours (son numero, son heure de depart, combien de produits deja
    # parcourus, la duree des cycles precedents). Un run seul ne sait rien
    # du cycle ; c'est ce fichier qui fait la memoire longue.
    etat = {}
    s = git("show", f"FETCH_HEAD:{ETAT}")
    if s.returncode == 0:
        try:
            etat = json.loads(s.stdout) or {}
        except Exception:
            etat = {}
    if "dernier" not in etat:
        # rattrapage depuis l'ancien curseur.txt, pour que le passage a
        # etat.json ne fasse pas repartir le tour de zero
        s = git("show", f"FETCH_HEAD:{CURSEUR}")
        if s.returncode == 0 and s.stdout.strip().lstrip("-").isdigit():
            etat["dernier"] = int(s.stdout.strip())

    dernier = etat.get("dernier")
    print(f"  memoire reprise : {len(ordres)} ordres, "
          f"{len(agg)} heures en cours, {len(volp)} paliers de prix, "
          f"{len(pxh)} heures de prix"
          + (f", curseur apres le produit {dernier}" if dernier is not None
             else ", pas de curseur (on repart du debut)"))
    return ordres, agg, volp, pxh, instant, etat


def flt(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


EN_DETAIL = ["kind", "prix", "saturation", "restaurant"]

def lire_detail():
    """Prix de detail et saturation. Un echec n'est pas grave : le tableau
    de bord garde l'instantane embarque dans jeu.json."""
    try:
        d = fetch(DETAIL_URL % REALM, tries=1, cadence=True)
    except Exception:
        return None
    if not isinstance(d, list):
        return None
    lignes = []
    for e in d:
        if not isinstance(e, dict) or e.get("quality") is not None:
            continue
        p = e.get("averagePrice") or 0
        hist = e.get("retailData") or []
        # les ventes en restaurant : un volume par JOUR, pour tout le serveur
        resto = 0
        if hist and isinstance(hist[-1], dict):
            resto = int(hist[-1].get("amountSoldRestaurant") or 0)
        if p <= 0 and not resto:
            continue
        lignes.append([e.get("dbLetter"), round(float(p), 4),
                       round(float(e.get("saturation") or 1), 6), resto])
    lignes.sort(key=lambda r: r[0])
    return lignes or None


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

    Les doublons DEJA presents dans le fichier se combinent entre eux (un
    heritage de l'epoque ou plusieurs processus ecrivaient chacun leur
    morceau). Mais une heure qu'on REECRIT aujourd'hui remplace la
    precedente au lieu de s'y ajouter : l'agregat en memoire porte la
    totalite de l'heure — les quinze lectures de chaque produit y sont deja
    cumulees — donc l'additionner a ce qui existe deja compterait tout en
    double. C'est ce qui rend une reprise apres echec de push inoffensive."""
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
        table[cle(l)] = l            # remplace : voir la docstring

    with open(chemin, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(entete)
        w.writerows(sorted(table.values(),
                           key=lambda x: (x[0], int(x[1]), int(x[2]))))


def pousser_main(message):
    """Renvoie True si l'historique est REELLEMENT arrive sur GitHub.

    Ce booleen n'est pas decoratif : c'est lui qui autorise l'appelant a
    oublier l'heure qu'il vient d'archiver. Tant qu'il est faux, l'heure
    doit rester en memoire, sinon elle n'existe plus nulle part."""
    git("add", "data")
    if git("diff", "--staged", "--quiet").returncode == 0:
        return True                     # rien a envoyer : rien a perdre
    git("commit", "-m", message)
    for i in range(5):
        git("pull", "--rebase", "--autostash", "origin", BRANCHE)
        if git("push").returncode == 0:
            print("  historique enregistre")
            return True
        time.sleep(5 + i * 5)
    print("  ! ECHEC de l'enregistrement de l'historique")
    return False


# ------------------------------------------------------------- traitement

# ressource -> numeros d'ordres, tenu a jour en meme temps que le carnet
PAR_KIND = {}


def reindexer(ordres):
    PAR_KIND.clear()
    for oid, v in ordres.items():
        PAR_KIND.setdefault(v["kind"], set()).add(oid)


def traiter(kind, book, ordres, agg, volp, ts):
    """Compare le carnet recu a ce qu'on avait, et en tire les ventes.

    Renvoie le nombre d'EVENEMENTS observes : offres apparues, entamees ou
    disparues. C'est la mesure d'activite qui pilote la cadence de lecture —
    sans dimension, donc comparable entre l'electricite et les boules de
    Noel, et directement liee a ce qui abime la deduction du front."""
    evts = 0
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
        # une disparition suivie d'une apparition.
        #
        # Encore faut-il le PROUVER. L'ancienne regle se contentait de "ce
        # vendeur a une offre neuve quelque part sur cette qualite" et
        # reversait toute la quantite disparue en repose. Un gros producteur
        # qui annule une vieille offre chere ET met en vente sa production du
        # jour cochait la case sans avoir rien repositionne.
        #
        # On garde donc, pour chaque offre neuve, de quoi verifier :
        #   - son prix, qui doit DIFFERER de celui de l'offre disparue
        #     (repositionner, c'est changer de prix ; sinon ce n'est pas un
        #     repositionnement)
        #   - sa date de mise en vente donnee par le jeu, qui doit etre
        #     POSTERIEURE au dernier instant ou l'on a vu l'ancienne
        #   - sa quantite, pour n'appeler "repose" que ce qui est vraiment
        #     revenu en vente
        neufs_par_vendeur = {}
        for x in offres:
            if str(x["id"]) in ordres:
                continue
            v = str((x.get("seller") or {}).get("id", ""))
            if v:
                neufs_par_vendeur.setdefault(v, []).append(
                    {"qt": float(x["quantity"]), "p": float(x["price"]),
                     "pose_le": iso(x.get("posted"))})

        # --- ce qui a bouge -------------------------------------------
        for oid in anciens_par_q.get(q, []):
            av = ordres[oid]
            x = neuf.get(oid)
            if x is None:
                # offre entierement disparue
                evts += 1
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
                # A ce stade on SAIT que l'offre ne pouvait pas etre achetee
                # (une moins chere est restee intacte) : elle a donc ete
                # retiree. Reste a dire si son vendeur l'a remise en vente.
                candidates = [c for c in neufs_par_vendeur.get(av["sid"], [])
                              if abs(c["p"] - av["p"]) > 1e-9          # autre prix
                              and c["pose_le"] and c["pose_le"] >= av["passage"]]
                if candidates:
                    # on apparie avec la plus proche en quantite : c'est
                    # l'appariement le plus vraisemblable, et surtout il ne
                    # depend pas de l'ordre du carnet
                    c = min(candidates, key=lambda c: abs(c["qt"] - av["qt"]))
                    neufs_par_vendeur[av["sid"]].remove(c)
                    revenu = min(c["qt"], av["qt"])
                    b["repose"] += revenu
                    # 15 M retires et 10 M reposes, ce sont 10 M de
                    # repositionnement ET 5 M de retrait sec. Tout mettre en
                    # repose ferait disparaitre du marche 5 M sans le dire.
                    if av["qt"] > revenu:
                        b["retire"] += av["qt"] - revenu
                else:
                    b["retire"] += av["qt"]
                continue
            qt = float(x["quantity"])
            delta = av["qt"] - qt
            if delta:
                evts += 1
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
            evts += 1
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

    return evts


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

def duree_texte(sec):
    """Une duree que l'oeil lit sans compter les zeros."""
    if sec is None:
        return "—"
    sec = int(sec)
    if sec < 60:
        return f"{sec} s"
    if sec < 3600:
        return f"{sec // 60} min {sec % 60:02d} s"
    return f"{sec // 3600} h {(sec % 3600) // 60:02d}"


def barre(fait, total, largeur=24):
    """Une barre de progression en caracteres, parce qu'un pourcentage seul
    ne se compare pas d'un coup d'oeil d'un run a l'autre."""
    if total <= 0:
        return ""
    n = max(0, min(largeur, round(fait / total * largeur)))
    return "\u2588" * n + "\u2591" * (largeur - n)


def choisir(kinds, etat, taille, maintenant):
    """Qui lire, en TROIS RANGS stricts. Le run est toujours rempli : il n'y
    a jamais d'attente, seulement un ordre.

      RANG 1  age >= AGE_MAX          le plafond, absolu. On ne perd de vue
                                      aucun produit, quoi qu'il arrive.
      RANG 2  a bouge et age >= AGE_BOUGE   le flux qu'on veut capter.
      RANG 3  tout le reste, par score = (debit + PLANCHER) x age.

    POURQUOI DES RANGS ET PAS UN TRI PAR RETARD
    Trier tout le monde par "combien de retard sur mon echeance" donne la
    priorite aux volatils : leur echeance est plus courte (AGE_BOUGE), donc
    ils accumulent du retard plus vite. Un produit qui bouge a 4 min 40
    (40 s de retard sur 4 min) passerait devant un produit a 5 min 10 (10 s
    de retard sur 5 min). Une dizaine de volatils tourneraient en boucle
    pendant que les autres s'entassent au plafond. Le rang 1 rend cela
    impossible : le plafond passe TOUJOURS avant le flux.

    ANTICIPATION
    On ne prend pas un produit quand il a depasse son echeance, mais quand
    il va la depasser avant qu'on ait une nouvelle occasion de le lire —
    d'ou la marge, calee sur deux intervalles de run et mesuree en direct.
    Sans elle, on arriverait systematiquement en retard d'un run."""
    debit = etat.get("debit") or {}
    vu = etat.get("vu") or {}
    prix, prix_lu = etat.get("prix") or {}, etat.get("prix_lu") or {}
    marge = 2 * float(etat.get("intervalle_run") or 20)

    r1, r2, r3 = [], [], []
    for k in kinds:
        c = str(k)
        t_vu = float(vu.get(c, 0) or 0)
        age = (maintenant - t_vu) if t_vu else 1e9      # jamais lu : prioritaire
        d = float(debit.get(c, 0) or 0)
        p, p0 = prix.get(c), prix_lu.get(c)
        bouge = (p is not None and p0 is not None
                 and abs(float(p) - float(p0)) > 1e-9)
        if age >= AGE_MAX - marge:
            r1.append((age, d, k))
        elif bouge and age >= AGE_BOUGE - marge:
            r2.append((age, d, k))
        else:
            score = (d + PLANCHER) * (age / 60.0) * (BONUS_TICKER if bouge else 1)
            r3.append((score, age, k))
    # dans chaque rang : le plus vieux d'abord ; a age egal, le plus actif
    r1.sort(reverse=True); r2.sort(reverse=True); r3.sort(reverse=True)
    choix = [k for _, _, k in r1] + [k for _, _, k in r2] + [k for _, _, k in r3]
    return choix[:min(taille, len(kinds))]


def faisabilite(kinds, etat, bouges):
    """Les echeances demandees tiennent-elles dans le debit disponible ?

    Une echeance n'est pas un souhait : c'est une charge. Exiger une lecture
    toutes les T minutes pour N produits coute N/T lectures par minute. Si
    la somme depasse ce que la chaine sait faire, la promesse est fausse et
    il vaut mieux le dire que le decouvrir dans les donnees."""
    n = len(kinds)
    cap = None
    if etat.get("cycles_faits"):
        cap = n / (etat["total_sec"] / etat["cycles_faits"] / 60.0)
    # les produits en mouvement sont tenus a AGE_BOUGE, les autres a AGE_MAX
    besoin = (bouges / (AGE_BOUGE / 60.0)
              + max(n - bouges, 0) / (AGE_MAX / 60.0))
    return {"cap": cap, "besoin": besoin,
            "tient": cap is None or besoin <= cap,
            "marge": (cap - besoin) if cap else None}


def stats_collecte(kinds, etat, maintenant):
    """Ou en est la collecte des 142, en une passe.

    C'est le tableau de bord du selecteur : si l'allocation derape, c'est
    ici que ca se voit — un age median qui gonfle, une queue de produits
    oublies, ou des mouvements signales que l'on n'arrive plus a suivre."""
    vu = etat.get("vu") or {}
    prix, prix_lu = etat.get("prix") or {}, etat.get("prix_lu") or {}
    ages, bouges, retard, jamais = [], 0, [], 0
    for k in kinds:
        c = str(k)
        if not vu.get(c):
            jamais += 1
            continue
        age = maintenant - float(vu[c])
        ages.append((age, k))
        p, p0 = prix.get(c), prix_lu.get(c)
        if p is not None and p0 is not None and abs(float(p) - float(p0)) > 1e-9:
            bouges += 1
            if age >= AGE_BOUGE:
                retard.append((age, k))
    ages.sort()
    tranches = [("moins de 1 min", 0, 60), ("1 a 3 min", 60, 180),
                ("3 a 5 min", 180, 300), ("5 a 10 min", 300, 600),
                ("plus de 10 min", 600, float("inf"))]
    seaux = [(nom, sum(1 for a, _ in ages if a0 <= a < a1))
             for nom, a0, a1 in tranches]
    med = ages[len(ages) // 2][0] if ages else 0
    return {"ages": ages, "median": med, "seaux": seaux, "jamais": jamais,
            "vieux": ages[-1] if ages else (0, None),
            "bouges": bouges, "retard": sorted(retard, reverse=True)}


def noter_lecture(etat, k, evts, maintenant):
    """Met a jour le debit du produit apres l'avoir lu.

    Moyenne mobile exponentielle : le dernier releve pese MEMOIRE, le passe
    le reste. Assez reactif pour suivre un reveil de marche, assez lent pour
    ne pas s'emballer sur une lecture creuse."""
    c = str(k)
    vu = etat.setdefault("vu", {})
    debit = etat.setdefault("debit", {})
    ecoule = max((maintenant - float(vu.get(c, 0) or 0)) / 60.0, 1 / 60.0)
    mesure = evts / ecoule                                # evenements par minute
    if c in debit and vu.get(c):
        debit[c] = round(MEMOIRE * mesure + (1 - MEMOIRE) * float(debit[c]), 4)
    else:
        debit[c] = round(mesure, 4)                       # premiere fois
    vu[c] = round(maintenant, 1)
    # le prix du ticker au moment de cette lecture : reference pour savoir,
    # au prochain tour, si quelque chose a bouge depuis
    p = (etat.get("prix") or {}).get(c)
    if p is not None:
        etat.setdefault("prix_lu", {})[c] = p


def main():
    debut = time.time()
    ordres, agg, volp, pxh, instant, etat = charger_live()
    reindexer(ordres)

    # --- ce run fait-il le releve de prix ? --------------------------
    etat["run_no"] = etat.get("run_no", 0) + 1
    # L'intervalle reel entre deux runs, mesure en direct : il sert de marge
    # d'anticipation au selecteur. Moyenne mobile, pour absorber un run lent
    # sans se laisser deregler par lui.
    prec = float(etat.get("dernier_run") or 0)
    if prec and 1 < debut - prec < 600:
        etat["intervalle_run"] = round(0.3 * (debut - prec)
                                       + 0.7 * float(etat.get("intervalle_run")
                                                     or (debut - prec)), 1)
    etat["dernier_run"] = round(debut, 1)
    en_cache = [int(k) for k in etat.get("kinds", [])]
    fait_ticker = (TICKER_RUNS <= 1 or etat["run_no"] % TICKER_RUNS == 1
                   or not en_cache)
    if fait_ticker:
        tk = fetch(TICKER, tries=3, cadence=True)
        kinds = (sorted({int(r["kind"]) for r in tk}) if tk
                 else en_cache or list(range(1, 156)))
        if tk:
            traiter_ticker(tk, pxh, instant, stamp())
            AVANCEE["tickers"] += 1
    else:
        kinds = en_cache
    etat["kinds"] = kinds
    # le prix courant de chaque produit, garde pour le selecteur du prochain
    # run : c'est lui qui dit "ca a bouge depuis ta derniere lecture"
    etat["prix"] = {str(k): v["p"] for k, v in instant.items() if v.get("p")}

    if not etat.get("cycle"):
        etat.update(cycle=1, cycle_debut=debut, cycles_faits=0,
                    total_sec=0.0, dernier_cycle_sec=None)

    # --- qui lit-on ? -------------------------------------------------
    taille = LOT + (0 if fait_ticker else 1)
    lot = choisir(kinds, etat, taille, debut)
    sup = [k for k in sorted(SUIVIS) if k in kinds and k not in lot]
    a_lire = sup + lot
    AVANCEE["total"] = len(a_lire)

    # --- ou en est-on du cycle ? --------------------------------------
    # Un cycle = tous les produits lus au moins une fois. Avec un choix
    # adaptatif il n'y a plus de "lot 3 sur 16" : on compte simplement
    # combien de produits ont ete vus depuis le debut du cycle. L'info est
    # deja dans `vu`, rien de plus a memoriser.
    vu = etat.get("vu") or {}
    debut_cycle = float(etat.get("cycle_debut", debut))
    faits = sum(1 for k in kinds if float(vu.get(str(k), 0) or 0) >= debut_cycle)

    budget = len(a_lire) + (1 if fait_ticker else 0)
    debit = etat.get("debit") or {}
    print(f"{len(kinds)} produits · {len(lot)} carnets ce run "
          f"({'avec' if fait_ticker else 'sans'} releve de prix) · "
          f"cycle {etat['cycle']} a {faits}/{len(kinds)}")
    detail = ", ".join(
        f"{k}({float(debit.get(str(k), 0) or 0):.1f}/min"
        + (",age " + duree_texte(debut - float((vu or {}).get(str(k), 0) or 0))
           if vu.get(str(k)) else ",jamais lu") + ")"
        for k in lot)
    print(f"  choisis : {detail}")
    if sup:
        print(f"  + suivis : {', '.join(map(str, sup))}")
    if budget <= QUOTA[0]:
        print(f"  budget : {budget} requetes pour un quota de {QUOTA[0]} par "
              f"{FENETRE:.0f} s — aucune attente prevue")
    else:
        cout = (budget - QUOTA[0]) * FENETRE / QUOTA[0] + FENETRE
        print(f"  ! budget : {budget} requetes pour un quota de {QUOTA[0]} — "
              f"~{cout:.0f} s d'attente facturees pour rien. Baisser LOT a "
              f"{max(1, QUOTA[0] - 1 - len(sup))}.")

    # Photo AVANT la collecte : c'est l'etat que le selecteur a vu, donc
    # celui qui juge sa decision. La prendre apres flatterait le bilan.
    av = stats_collecte(kinds, etat, debut)
    fais = faisabilite(kinds, etat, av["bouges"])
    if fais["cap"] and not fais["tient"]:
        print(f"  ! ECHEANCES INTENABLES : elles exigent "
              f"{fais['besoin']:.1f} lectures/min, la chaine en fait "
              f"{fais['cap']:.1f}. Il manque {-fais['marge']:.1f}/min.")
        print(f"  ! remonter AGE_MAX_MIN (actuellement {AGE_MAX/60:.0f}) ou "
              f"AGE_BOUGE_MIN (actuellement {AGE_BOUGE/60:.0f}), "
              f"sinon les retards sont structurels.")
    if av["retard"]:
        pire = av["retard"][0]
        etat["retard_max_sec"] = max(float(etat.get("retard_max_sec", 0) or 0),
                                     round(pire[0]))
        print(f"  ! {len(av['retard'])} produit(s) en mouvement au-dela de "
              f"l'echeance de {AGE_BOUGE/60:.0f} min — le pire : produit "
              f"{pire[1]}, {duree_texte(pire[0])}")

    # --- la collecte ---------------------------------------------------
    for k in a_lire:
        if time.time() - debut > DUREE:
            print("  ! plafond de duree atteint, on ferme ici")
            break
        ts = stamp()
        book = fetch(BOOK % (REALM, k), tries=2, cadence=True)
        if book:
            t0 = time.time()
            evts = traiter(k, book, ordres, agg, volp, ts)
            CHRONO["traitement"] += time.time() - t0
            noter_lecture(etat, k, evts, time.time())
            AVANCEE["lues"] += 1
        else:
            # Lecture ratee : on ne touche NI a `vu` NI au debit. Son age
            # continue donc de grandir et il repassera en tete tout seul —
            # pas besoin de file d'attente pour les echecs.
            print(f"  carnet {k} indisponible, passe au suivant")

    if time.time() - debut >= TICKER_SEC and slot_libre():
        tkf = fetch(TICKER, tries=1, cadence=True)
        if tkf:
            traiter_ticker(tkf, pxh, instant, stamp())
            AVANCEE["tickers"] += 1
            etat["prix"] = {str(k): v["p"] for k, v in instant.items() if v.get("p")}

    etat["dernier"] = lot[-1] if lot else etat.get("dernier")

    # --- le cycle s'est-il referme ? ----------------------------------
    vu = etat.get("vu") or {}
    faits = sum(1 for k in kinds if float(vu.get(str(k), 0) or 0) >= debut_cycle)
    cycle_boucle = faits >= len(kinds)
    duree_cycle = None
    if cycle_boucle:
        duree_cycle = time.time() - debut_cycle
        etat["dernier_cycle_sec"] = round(duree_cycle)
        etat["cycles_faits"] = etat.get("cycles_faits", 0) + 1
        etat["total_sec"] = etat.get("total_sec", 0.0) + duree_cycle
        etat["cycle"] = etat.get("cycle", 1) + 1
        etat["cycle_debut"] = time.time()
        etat["cycle_debut_iso"] = stamp()
        etat["retard_max_sec"] = 0          # le bilan du cycle repart propre
        faits = 0

    t0 = time.time()
    fermer_et_envoyer(ordres, agg, volp, pxh, instant, etat, final=True)
    CHRONO["envoi"] += time.time() - t0

    ecoule = time.time() - debut
    complet = AVANCEE["lues"] == len(a_lire)
    print(f"fin de run en {ecoule:.0f} s : {AVANCEE['lues']}/{len(a_lire)} "
          f"carnets lus ({'lot complet' if complet else 'lot INCOMPLET'}), "
          f"{AVANCEE['tickers']} releves de prix, {N429[0]} refus")
    if cycle_boucle:
        print(f"  CYCLE {etat['cycle'] - 1} BOUCLE : les {len(kinds)} produits "
              f"ont tous ete lus, en {duree_texte(duree_cycle)}")
    print("  " + chrono_texte(ecoule))

    # --- ou en est la collecte des 142 ? -------------------------------
    ap = stats_collecte(kinds, etat, time.time())
    dbt = etat.get("debit") or {}
    actifs = sorted(kinds, key=lambda k: -float(dbt.get(str(k), 0) or 0))[:5]
    vieux, attente = ap["vieux"][1], ap["vieux"][0]
    print("  fraicheur des 142 : " + " · ".join(
        f"{nom} {n}" for nom, n in ap["seaux"] if n)
        + (f" · jamais lus {ap['jamais']}" if ap["jamais"] else ""))
    print(f"  age median {duree_texte(ap['median'])} · le plus vieux : produit "
          f"{vieux} ({duree_texte(attente)}, plafond {AGE_MAX/60:.0f} min)")
    print(f"  en mouvement selon le ticker : {ap['bouges']}/{len(kinds)}"
          f" · dont {len(ap['retard'])} au-dela de l'echeance")
    print("  les plus actifs : " + ", ".join(
        f"{k} ({float(dbt.get(str(k), 0) or 0):.1f} evt/min)" for k in actifs))

    # --- l'etiquette du run SUIVANT ------------------------------------
    suite = choisir(kinds, etat, taille, time.time())
    if suite:
        with open(ETIQUETTE, "w") as fh:
            fh.write(f"cycle {etat['cycle']} · {faits}/{len(kinds)} lus · "
                     f"suivants {', '.join(map(str, suite[:5]))}"
                     f"{'...' if len(suite) > 5 else ''}\n")

    # --- le resume sur la PAGE du run ----------------------------------
    resume = os.environ.get("GITHUB_STEP_SUMMARY")
    if resume:
        sante = ("a verifier" if (not complet or N429[0] or ap["retard"]
                                  or not fais["tient"]) else "OK")
        total = len(kinds)
        depuis = time.time() - float(etat.get("cycle_debut", debut))
        moy = (etat["total_sec"] / etat["cycles_faits"]
               if etat.get("cycles_faits") else None)
        reste = depuis / faits * (total - faits) if faits and not cycle_boucle else None
        cap = total / (moy / 60) if moy else None      # carnets par minute
        with open(resume, "a") as fh:
            fh.write(
                f"## Cycle {etat['cycle']} — {sante}\n\n"
                f"`{barre(faits, total)}` **{faits}/{total}** produits lus dans "
                f"ce cycle · en cours depuis {duree_texte(depuis)}"
                + (f", fin estimee dans {duree_texte(reste)}" if reste else "")
                + "\n\n")

            # --- ou en est-on sur les 142 ? ---------------------------
            fh.write("### Fraicheur des 142 carnets\n\n| age | produits |\n"
                     "|---|---|\n")
            for nom, n in ap["seaux"]:
                fh.write(f"| {nom} | **{n}** {barre(n, total, 14) if n else ''} |\n")
            if ap["jamais"]:
                fh.write(f"| jamais lus | **{ap['jamais']}** |\n")
            fh.write(f"\nAge median **{duree_texte(ap['median'])}** · le plus "
                     f"vieux : produit **{vieux}** ({duree_texte(attente)}, "
                     f"plafond {AGE_MAX/60:.0f} min)\n\n")

            # --- la promesse est-elle tenue ? -------------------------
            fh.write("### Mouvement et echeance\n\n| | |\n|---|---|\n"
                     f"| Produits en mouvement (ticker) | {ap['bouges']}/{total} |\n"
                     f"| Au-dela de l'echeance de {AGE_BOUGE/60:.0f} min | "
                     + (", ".join(f"produit {k} ({duree_texte(a)})"
                                  for a, k in ap["retard"][:3])
                        if ap["retard"] else "aucun") + " |\n"
                     f"| Pire retard de ce cycle | "
                     f"{duree_texte(float(etat.get('retard_max_sec', 0) or 0))} |\n"
                     + (f"| Capacite mesuree | {cap:.1f} carnets/min |\n"
                        if cap else "")
                     + (f"| Charge exigee par les echeances | "
                        f"{fais['besoin']:.1f} lectures/min "
                        + ("— **ca ne tient pas**, il manque "
                           f"{-fais['marge']:.1f}/min"
                           if not fais["tient"] else
                           f"— il reste {fais['marge']:.1f}/min de marge")
                        + " |\n" if fais["cap"] else "")
                     + "\n")

            # --- ce run --------------------------------------------------
            fh.write("### Ce run\n\n| | |\n|---|---|\n"
                     f"| Carnets lus | {', '.join(map(str, lot))} |\n"
                     f"| Reussite | {AVANCEE['lues']}/{len(a_lire)}"
                     f"{'' if complet else ' — INCOMPLET'} |\n"
                     f"| Releves de prix | {AVANCEE['tickers']} |\n"
                     f"| Refus du serveur | {N429[0]} |\n"
                     f"| Duree | {ecoule:.0f} s |\n"
                     f"| Les plus actifs | "
                     + ", ".join(f"{k} ({float(dbt.get(str(k),0) or 0):.1f}/min)"
                                 for k in actifs) + " |\n"
                     + (f"| Cycle precedent | "
                        f"{duree_texte(etat['dernier_cycle_sec'])} |\n"
                        if etat.get("dernier_cycle_sec") else "")
                     + (f"| Moyenne sur {etat['cycles_faits']} cycle(s) | "
                        f"{duree_texte(moy)} |\n" if moy else "") + "\n")
            if cycle_boucle:
                fh.write(f"> Le cycle {etat['cycle'] - 1} vient de se refermer "
                         f"en {duree_texte(duree_cycle)}.\n\n")

    with open(TEMOIN, "w") as fh:
        fh.write(stamp() + "\n")

def fermer_et_envoyer(ordres, agg, volp, pxh, instant, etat=None,
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
        # L'ORDRE COMPTE. Avant, on supprimait l'heure de la memoire PUIS on
        # poussait. Si le push echouait, l'heure n'existait plus ni en
        # memoire, ni sur live, ni sur main : elle etait perdue, en silence,
        # et le runner emportait les fichiers locaux avec lui.
        # Maintenant on ne l'oublie que si elle est arrivee a bon port. Sinon
        # elle reste dans le carnet vivant et repart au run suivant.
        if pousser_main("heures " + ", ".join(finies)):
            for cle in [c for c in pxh if c[0] in finies]:
                del pxh[cle]
            for cle in [c for c in agg if c[0] in finies]:
                del agg[cle]
            for cle in [c for c in volp if c[0] in finies]:
                del volp[cle]
            print(f"  {len(finies)} heure(s) archivee(s) : {', '.join(finies)}")
        else:
            print(f"  ! {len(finies)} heure(s) NON archivee(s), gardee(s) en "
                  f"memoire pour le prochain run : {', '.join(finies)}")

    fichiers = {
        "ordres.csv": en_csv(EN_ORDRES, lignes_ordres(ordres)),
        "heure_horaire.csv": en_csv(EN_HORAIRE, lignes_horaire(agg)),
        "heure_volume.csv": en_csv(EN_VOLUME, lignes_volume(volp)),
        "heure_prix.csv": en_csv(EN_PRIX, lignes_prix(pxh)),
        "prix.csv": en_csv(EN_INSTANT, lignes_instant(instant)),
    }
    # Le detail ne bouge qu'une fois par jour : on ne le relit qu'au dernier
    # envoi d'un run, et un echec laisse simplement le fichier tel quel.
    if final:
        det = lire_detail()
        if det:
            fichiers["detail.csv"] = en_csv(EN_DETAIL, det)
            print(f"  detail : {len(det)} prix de vente au detail")
    # L'etat voyage avec le carnet, dans le MEME commit : impossible
    # d'enregistrer les donnees sans enregistrer ou on en est, ou l'inverse.
    if etat:
        fichiers[ETAT] = json.dumps(etat, indent=1, sort_keys=True) + "\n"
        if etat.get("dernier") is not None:
            fichiers[CURSEUR] = f"{etat['dernier']}\n"
    ok = pousser_live(fichiers)
    if ok:
        print(f"  envoye : {len(instant)} prix a la minute, "
              f"{len(ordres)} ordres" +
              (" — dernier envoi" if final else ""))
        journal_avancee(force=True)


if __name__ == "__main__":
    main()
