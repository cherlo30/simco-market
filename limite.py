"""Les deux points d'acces ont-ils la MEME limite ?

Observation faite dans le navigateur : le jeu lui-meme n'appelle jamais
/market/all/. Sa page de marche demande /market/0/<produit>/ — 200 offres,
74 ko — la ou /market/all/0/<produit>/ rend le carnet entier, 481 offres,
190 ko. Toutes nos mesures de limite ont ete faites sur la version lourde.

Il est courant qu'un serveur soit plus severe sur un point d'acces couteux
que personne n'utilise en jeu. Si c'est le cas ici, changer d'URL suffit.

Quatre mesures, en alternance pour que l'heure ne fausse pas la comparaison.
"""
import urllib.request, urllib.error, time, os, itertools

REALM = 0
BASE = "https://www.simcompanies.com"
LEGER = f"/api/v3/market/{REALM}/%d/"        # ce que le jeu utilise
LOURD = f"/api/v3/market/all/{REALM}/%d/"    # ce que nous utilisons
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"
REPOS = float(os.environ.get("REPOS", "90"))

VIVIER = itertools.cycle(range(1, 156))
_deja = set()
total = [0]


def neuve():
    for k in VIVIER:
        if k not in _deja:
            _deja.add(k)
            return k
    _deja.clear()
    return neuve()


def un(motif):
    total[0] += 1
    t = time.time()
    req = urllib.request.Request(BASE + motif % neuve(),
                                 headers={"User-Agent": UA,
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            n = len(r.read())
        return 200, time.time() - t, n
    except urllib.error.HTTPError as e:
        try:
            e.read()
        except Exception:
            pass
        return e.code, time.time() - t, 0
    except Exception:
        return -1, time.time() - t, 0


def capacite(motif, plafond=30):
    ok = octets = 0
    for _ in range(plafond):
        c, d, n = un(motif)
        if c != 200:
            return ok, octets
        ok += 1
        octets += n
    return ok, octets


def tenue(motif, ecart, combien=30):
    """Combien passent quand on tient un rythme regulier."""
    ok = ko = 0
    t0 = time.time()
    for i in range(combien):
        c, d, n = un(motif)
        if c == 200:
            ok += 1
        else:
            ko += 1
        cible = t0 + (i + 1) * ecart
        time.sleep(max(0.0, cible - time.time()))
    return ok, ko, time.time() - t0


def repos(quoi=""):
    print(f"    (repos {REPOS:.0f} s{' — ' + quoi if quoi else ''})", flush=True)
    time.sleep(REPOS)


print("=" * 66)
print("LEGER  /api/v3/market/0/N/       — ce que le jeu utilise (200 offres)")
print("LOURD  /api/v3/market/all/0/N/   — ce que nous utilisons (carnet entier)")
print("=" * 66, flush=True)

print("\n[1] Capacite d'affilee apres un long silence", flush=True)
resultats = {}
for nom, motif in (("LOURD", LOURD), ("LEGER", LEGER),
                   ("LEGER", LEGER), ("LOURD", LOURD)):
    repos("remise a zero")
    ok, octets = capacite(motif)
    resultats.setdefault(nom, []).append(ok)
    print(f"    {nom} : {ok} requetes d'affilee"
          f"  ({octets//1024} Ko au total)", flush=True)
for nom, v in resultats.items():
    print(f"    -> {nom} : {v} (median {sorted(v)[len(v)//2]})")

print("\n[2] Rythme tenu : 30 requetes espacees de 2 s", flush=True)
for nom, motif in (("LOURD", LOURD), ("LEGER", LEGER)):
    repos("remise a zero")
    ok, ko, d = tenue(motif, 2.0)
    print(f"    {nom} : {ok} passees, {ko} refusees en {d:.0f} s"
          f"  -> {ok/d*60:.1f} lues/min", flush=True)

print("\n[3] Rythme tenu : 30 requetes espacees de 1 s", flush=True)
for nom, motif in (("LEGER", LEGER), ("LOURD", LOURD)):
    repos("remise a zero")
    ok, ko, d = tenue(motif, 1.0)
    print(f"    {nom} : {ok} passees, {ko} refusees en {d:.0f} s"
          f"  -> {ok/d*60:.1f} lues/min", flush=True)

print("\n" + "=" * 66)
print(f"requetes totales : {total[0]}")
print("Si LEGER passe nettement mieux, il suffit de changer d'URL :")
print("  142 produits / (lues par minute) = duree du cycle")
print("=" * 66)
