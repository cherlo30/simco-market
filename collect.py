#!/usr/bin/env python3
"""Collecteur continu du marche Sim Companies.

Balaye sans arret les carnets d'ordres des ~155 ressources. Au lieu de
reenregistrer tout le carnet a chaque tour — 95 % des ordres n'ont pas bouge —
il n'ecrit que les MOUVEMENTS : ordre apparu, quantite reduite (une vente),
ordre disparu. Dix-neuf fois moins de volume, et l'horodatage exact de chaque
evenement.

Sorties
  data/tape/AAAA-MM/AAAA-MM-JJ.csv.gz   la bande des mouvements
  data/book/AAAA-MM-JJ.csv              un resume par tour, ressource ET qualite
  data/ticker/AAAA-MM-JJ.csv            le prix des 155 ressources, ~30 s

Toutes les qualites (0 a 12 etoiles) sont suivies : une meme ressource en Q4
se vend couramment 20 a 25 % plus cher qu'en Q0.
"""
import csv, functools, gzip, json, os, statistics, subprocess, sys, time
import urllib.request, urllib.error

print = functools.partial(print, flush=True)   # journal lisible en direct
from datetime import datetime, timezone

REALM = 0
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
BOOK = "https://www.simcompanies.com/api/v3/market/all/%d/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/2.0)"

DELAY = float(os.environ.get("DELAY", "1.3"))          # entre deux requetes
TICKER_SEC = int(os.environ.get("TICKER_SEC", "30"))   # frequence du ticker
DUREE = int(os.environ.get("DUREE_MIN", "330")) * 60   # duree de vie du process
COMMIT_SEC = int(os.environ.get("COMMIT_SEC", "600"))  # sauvegarde reguliere


# Frein automatique. Le serveur du jeu repond 429 ("trop de requetes") quand
# on tape trop vite. A chaque 429 on ralentit un peu ; quand tout passe, on
# reaccelere doucement. Le collecteur trouve tout seul le rythme accepte.
FREIN = [0.0]        # secondes ajoutees a DELAY entre deux requetes
N429 = [0]           # compteur, pour le journal


def fetch(url, tries=4):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode())
            FREIN[0] = max(0.0, FREIN[0] - 0.02)      # on relache doucement
            return d
        except urllib.error.HTTPError as e:
            if e.code == 429:
                N429[0] += 1
                FREIN[0] = min(4.0, FREIN[0] + 0.3)   # on ralentit
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


def age_h(iso, now):
    try:
        return round((now - datetime.fromisoformat(iso.replace("Z", "+00:00"))
                      ).total_seconds() / 3600.0, 3)
    except Exception:
        return ""


def entete_ok(chemin, entete, gz=False):
    """Vrai si le fichier existe DEJA avec exactement cet en-tete.

    Si l'en-tete a change (nouvelle colonne, comme les etoiles de qualite),
    on met l'ancien fichier de cote sous un autre nom au lieu d'ecrire des
    lignes a 9 colonnes sous un en-tete a 8 : melangees, elles rendent tout
    le fichier illisible. Chaque fichier garde ainsi un seul format.
    """
    if not os.path.exists(chemin):
        return False
    attendu = ",".join(entete)
    try:
        ouvrir = gzip.open if gz else open
        with ouvrir(chemin, "rt", newline="") as fh:
            premiere = fh.readline().strip("\r\n")
    except Exception:
        premiere = None
    if premiere == attendu:
        return True
    base, ext = chemin, ""
    if base.endswith(".csv.gz"):
        base, ext = base[:-7], ".csv.gz"
    elif base.endswith(".csv"):
        base, ext = base[:-4], ".csv"
    n = 1
    while os.path.exists(f"{base}.ancien{n}{ext}"):
        n += 1
    os.rename(chemin, f"{base}.ancien{n}{ext}")
    print(f"  en-tete different : {chemin} mis de cote en "
          f"{base}.ancien{n}{ext}, nouveau fichier propre")
    return False


class Sortie:
    """Gere les trois fichiers du jour, reouverts au changement de date."""

    def __init__(self):
        self.jour = None

    def ouvrir(self, now):
        j = now.strftime("%Y-%m-%d")
        if j == self.jour:
            return
        self.fermer()
        self.jour = j
        mois = now.strftime("%Y-%m")
        os.makedirs(f"data/tape/{mois}", exist_ok=True)
        os.makedirs("data/book", exist_ok=True)
        os.makedirs("data/ticker", exist_ok=True)

        e = ["ts", "kind", "quality", "evt", "order_id",
             "price", "qty", "delta", "seller_id", "reprise"]
        p = f"data/tape/{mois}/{j}.csv.gz"
        neuf = not entete_ok(p, e, gz=True)
        self.f_tape = gzip.open(p, "at", newline="")
        self.w_tape = csv.writer(self.f_tape)
        if neuf:
            self.w_tape.writerow(e)

        e = ["ts", "kind", "quality", "best", "qty_best", "n_orders",
             "total_qty", "qty_within_5pct", "median_age_h"]
        p = f"data/book/{j}.csv"
        neuf = not entete_ok(p, e)
        self.f_book = open(p, "a", newline="")
        self.w_book = csv.writer(self.f_book)
        if neuf:
            self.w_book.writerow(e)

        e = ["ts", "kind", "price", "is_up"]
        p = f"data/ticker/{j}.csv"
        neuf = not entete_ok(p, e)
        self.f_tick = open(p, "a", newline="")
        self.w_tick = csv.writer(self.f_tick)
        if neuf:
            self.w_tick.writerow(e)

    def fermer(self):
        for a in ("f_tape", "f_book", "f_tick"):
            if hasattr(self, a):
                getattr(self, a).close()

    def vider(self):
        for a in ("f_tape", "f_book", "f_tick"):
            if hasattr(self, a):
                getattr(self, a).flush()

    def boucler(self, now):
        """Ferme et rouvre les fichiers : sur le disque ils sont alors
        complets et valides, prets a etre enregistres."""
        self.fermer()
        self.jour = None
        self.ouvrir(now)


def sauvegarder():
    """Enregistre le travail fait jusqu'ici, pour ne rien perdre si le job
    est interrompu."""
    def sh(*a):
        return subprocess.run(a, capture_output=True, text=True).returncode
    # On refait la synthese horaire avant chaque enregistrement : sans ca, le
    # fichier data/hourly ne serait rafraichi qu'a la fin de la course, soit
    # toutes les 5 h 30 — le tableau de bord afficherait des donnees perimees.
    r = subprocess.run([sys.executable, "rollup.py"], capture_output=True, text=True)
    if r.returncode != 0:
        print("  ! synthese horaire en echec : " +
              (r.stderr.strip().splitlines() or ["?"])[-1])
    sh("git", "add", "data")
    if subprocess.run(["git", "diff", "--staged", "--quiet"]).returncode == 0:
        return
    sh("git", "commit", "-m", "collecte " + stamp())
    br = os.environ.get("GITHUB_REF_NAME", "main")
    for i in range(5):
        sh("git", "pull", "--rebase", "--autostash", "origin", br)
        if sh("git", "push") == 0:
            print("  enregistre")
            return
        time.sleep(5 + i * 5)
    print("  ECHEC de l'enregistrement")


def main():
    etat = {}                 # kind -> {order_id: (prix, quantite)}
    out = Sortie()
    fin = time.time() + DUREE
    dernier_ticker = 0.0
    rates_tick = [0]
    dernier_commit = time.time()
    tour = 0
    evts = 0

    kinds = sorted({r["kind"] for r in (fetch(TICKER) or [])})
    if not kinds:
        raise SystemExit("ticker injoignable, on arrete")
    print(f"{len(kinds)} ressources suivies, un tour toutes les "
          f"~{len(kinds)*DELAY/60:.1f} min")

    while time.time() < fin:
        tour += 1
        t0 = time.time()
        for k in kinds:
            if time.time() > fin:
                break
            now = datetime.now(timezone.utc)
            out.ouvrir(now)

            # le ticker, une requete, intercale toutes les ~30 s
            if time.time() - dernier_ticker > TICKER_SEC:
                tk = fetch(TICKER, tries=2)
                if tk:
                    dernier_ticker = time.time()
                    rates_tick[0] = 0
                    ts = stamp(now)
                    for r in tk:
                        out.w_tick.writerow([ts, r["kind"], r["price"],
                                             int(bool(r.get("is_up")))])
                else:
                    # on NE met PAS a jour le compteur : nouvel essai des la
                    # ressource suivante, au lieu d'attendre 30 s de plus
                    rates_tick[0] += 1
                    if rates_tick[0] in (1, 5, 20, 100):
                        print(f"  ticker indisponible ({rates_tick[0]} echecs "
                              f"d'affilee) — on continue et on reessaie")
                time.sleep(DELAY + FREIN[0])

            book = fetch(BOOK % (REALM, k))
            time.sleep(DELAY + FREIN[0])
            if book is None:
                continue
            now = datetime.now(timezone.utc)
            ts = stamp(now)
            book.sort(key=lambda x: x["price"])

            # --- resume du carnet, une ligne par niveau de qualite
            par_q = {}
            for x in book:
                par_q.setdefault(x.get("quality", 0), []).append(x)
            for q in sorted(par_q):
                sous = par_q[q]
                best = sous[0]["price"]
                ages = [v for v in (age_h(x.get("posted") or "", now)
                                    for x in sous[:20]) if v != ""]
                out.w_book.writerow([
                    ts, k, q, best,
                    sum(x["quantity"] for x in sous if x["price"] == best),
                    len(sous), sum(x["quantity"] for x in sous),
                    sum(x["quantity"] for x in sous if x["price"] <= best * 1.05),
                    round(statistics.median(ages), 3) if ages else ""])

            # --- mouvements depuis le tour precedent
            neuf = {str(x["id"]): (x["price"], x["quantity"],
                                   x.get("quality", 0)) for x in book}
            vendeurs = {str(x["id"]): (x.get("seller") or {}).get("id", "")
                        for x in book}
            ancien = etat.get(k)
            reprise = 1 if ancien is None else 0
            ancien = ancien or {}

            for oid, (p, qt, ql) in neuf.items():
                if oid not in ancien:
                    out.w_tape.writerow([ts, k, ql, "N", oid, p, qt, "",
                                         vendeurs[oid], reprise]); evts += 1
                else:
                    ap, aq, _ = ancien[oid]
                    if qt != aq:
                        out.w_tape.writerow([ts, k, ql, "C", oid, p, qt, aq - qt,
                                             vendeurs[oid], 0]); evts += 1
                    if p != ap:
                        out.w_tape.writerow([ts, k, ql, "P", oid, p, qt, "",
                                             vendeurs[oid], 0]); evts += 1
            for oid, (p, qt, ql) in ancien.items():
                if oid not in neuf:
                    out.w_tape.writerow([ts, k, ql, "X", oid, p, 0, qt, "", 0])
                    evts += 1
            etat[k] = neuf

        out.vider()
        print(f"tour {tour} — {(time.time()-t0)/60:.1f} min, "
              f"{evts} mouvements cumules, "
              f"dernier ticker il y a {int(time.time()-dernier_ticker)} s, "
                  f"frein {FREIN[0]:.2f} s, {N429[0]} refus 429")

        if time.time() - dernier_commit > COMMIT_SEC:
            out.boucler(datetime.now(timezone.utc))
            sauvegarder()
            dernier_commit = time.time()

    out.fermer()
    print(f"\n{tour} tours, {evts} mouvements enregistres")


if __name__ == "__main__":
    main()
