"""Sonde v2 — corriger la faute de la v1 et trancher.

La v1 reutilisait les MEMES ressources pour vider la reserve et pour mesurer.
Avec un Cloudflare devant le jeu, la deuxieme serie pouvait etre servie en
cache et ne jamais atteindre le compteur : la table de recharge etait donc
inexploitable. Ici, chaque requete porte sur une ressource JAMAIS demandee
dans l'experience, et on lit cf-cache-status pour le verifier.

  A. le cache existe-t-il, et fausse-t-il la mesure ?
  B. combien de temps dure le blocage apres un refus ?
  C. quelle fenetre : 10 requetes par combien de secondes ?
  D. quel espacement tient indefiniment sans jamais un seul refus ?
"""
import urllib.request, urllib.error, time, os, itertools, json

REALM = 0
BOOK = f"https://www.simcompanies.com/api/v3/market/all/{REALM}/%d/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"
REPOS = float(os.environ.get("REPOS", "90"))

# Un vivier de ressources : chaque requete de la sonde en consomme une neuve,
# donc aucune URL n'est demandee deux fois. Plus de cache possible.
VIVIER = itertools.cycle(range(1, 156))
_deja = set()


def neuve():
    for k in VIVIER:
        if k not in _deja:
            _deja.add(k)
            return k
    _deja.clear()          # on a fait le tour : on recommence
    return neuve()


total = [0]


def un(k=None):
    """(code, duree, cache) — cache = ce que Cloudflare dit de la reponse."""
    total[0] += 1
    k = neuve() if k is None else k
    t = time.time()
    req = urllib.request.Request(BOOK % k, headers={"User-Agent": UA,
                                                    "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
            cc = r.headers.get("cf-cache-status", "?")
        return 200, time.time() - t, cc
    except urllib.error.HTTPError as e:
        cc = e.headers.get("cf-cache-status", "?")
        ra = e.headers.get("Retry-After")
        try:
            e.read()
        except Exception:
            pass
        return e.code, time.time() - t, (cc + (f" retry-after={ra}" if ra else ""))
    except Exception as e:
        return -1, time.time() - t, str(e)[:60]


def vider(plafond=25):
    """Tire des ressources neuves jusqu'au premier refus. Renvoie combien
    sont passees."""
    ok = 0
    for _ in range(plafond):
        c, d, cc = un()
        if c == 200:
            ok += 1
        else:
            return ok, c, cc
    return ok, None, None


def repos(s, quoi=""):
    print(f"    (repos {s:.0f} s{' — ' + quoi if quoi else ''})", flush=True)
    time.sleep(s)


print("=" * 64)
print("SONDE v2 — chaque requete porte sur une ressource jamais demandee")
print("=" * 64, flush=True)

# ------------------------------------------------------------------ E
# LA question qui decide de toute la suite. Le serveur n'accorde qu'une
# dizaine de lectures par minute : si market-ticker donne les prix des 155
# ressources en UNE requete, on a tous les prix chaque minute pour un dixieme
# du budget, et on garde le reste pour les carnets detailles.
print("\n[E] Que contient market-ticker, en une seule requete ?", flush=True)
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
try:
    req = urllib.request.Request(TICKER, headers={"User-Agent": UA,
                                                  "Accept": "application/json"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=30) as r:
        brut = r.read()
        cc = r.headers.get("cf-cache-status", "?")
    tk = json.loads(brut.decode())
    total[0] += 1
    print(f"    {len(tk)} entrees, {len(brut)//1024} Ko, en {(time.time()-t)*1000:.0f} ms"
          f"  cache={cc}")
    print("    champs disponibles : " + ", ".join(sorted(tk[0])))
    for e in tk[:3]:
        print("      " + json.dumps(e, ensure_ascii=False)[:200])
    prix = [c for c in tk[0] if any(m in c.lower() for m in
            ("price", "prix", "cost", "value", "bid", "ask"))]
    print("    -> " + (f"champs de prix reperes : {', '.join(prix)}"
                       " — une requete suffit pour tous les prix"
                       if prix else
                       "aucun champ de prix : le ticker ne remplace pas les carnets"))
except Exception as e:
    print(f"    echec : {e}")

# ------------------------------------------------------------------ F
# Le budget est en REQUETES, pas en produits. Si une requete peut ramener
# plusieurs carnets, le cycle est divise d'autant — et le serveur travaille
# moins, pas plus. On essaie donc les formes d'URL groupees les plus
# naturelles, une par une, espacees pour ne pas manger le quota.
print("\n[F] Une requete peut-elle ramener plusieurs produits ?", flush=True)
FORMES = [
    ("un seul produit (reference)", f"/api/v3/market/all/{REALM}/1/"),
    ("sans preciser le produit",    f"/api/v3/market/all/{REALM}/"),
    ("produits separes par virgule", f"/api/v3/market/all/{REALM}/1,2,3/"),
    ("liste en parametre",          f"/api/v3/market/all/{REALM}/1/?kinds=1,2,3"),
    ("marche sans le mot all",      f"/api/v3/market/{REALM}/"),
]
base = "https://www.simcompanies.com"
for nom, chemin in FORMES:
    try:
        req = urllib.request.Request(base + chemin,
                                     headers={"User-Agent": UA,
                                              "Accept": "application/json"})
        t = time.time()
        with urllib.request.urlopen(req, timeout=30) as r:
            brut = r.read()
        total[0] += 1
        try:
            d = json.loads(brut.decode())
            forme = (f"liste de {len(d)} entrees" if isinstance(d, list)
                     else f"objet a {len(d)} cles")
            kinds = set()
            if isinstance(d, list):
                for e in d[:4000]:
                    if isinstance(e, dict) and "kind" in e:
                        kinds.add(e["kind"])
            sup = f", {len(kinds)} produits distincts" if kinds else ""
        except Exception:
            forme, sup = "reponse non-JSON", ""
        print(f"    200  {nom:<30} {len(brut)//1024:>4} Ko  {forme}{sup}",
              flush=True)
    except urllib.error.HTTPError as e:
        total[0] += 1
        print(f"    {e.code}  {nom:<30} refuse", flush=True)
    except Exception as e:
        print(f"    ---  {nom:<30} {str(e)[:40]}", flush=True)
    time.sleep(7)          # on reste sous les 10 requetes par minute

# ------------------------------------------------------------------ A
print("\n[A] Le cache fausse-t-il la mesure ?", flush=True)
repos(REPOS)
k = neuve()
c1, d1, cc1 = un(k)
c2, d2, cc2 = un(k)          # exactement la MEME url, tout de suite apres
c3, d3, cc3 = un()           # une url neuve
print(f"    ressource {k}, 1re fois  : {c1} en {d1*1000:4.0f} ms  cache={cc1}")
print(f"    ressource {k}, 2e fois   : {c2} en {d2*1000:4.0f} ms  cache={cc2}")
print(f"    ressource neuve         : {c3} en {d3*1000:4.0f} ms  cache={cc3}")
print("    -> " + ("le cache repond a la place du serveur : la v1 mesurait le"
                   " cache, pas la limite"
                   if "HIT" in (cc2 or "").upper()
                   else "pas de cache : les deux requetes atteignent le serveur"))

# ------------------------------------------------------------------ B
print("\n[B] Combien de temps dure le blocage apres un refus ?", flush=True)
repos(REPOS, "on repart d'une reserve pleine")
ok, code, cc = vider()
print(f"    {ok} passees, puis {code} ({cc})", flush=True)
t_refus = time.time()
debloque = None
for essai in range(40):                 # jusqu'a 200 s
    time.sleep(5)
    c, d, cc = un()
    ecoule = time.time() - t_refus
    print(f"    +{ecoule:5.0f} s : {c}", flush=True)
    if c == 200:
        debloque = ecoule
        break
print(f"    -> le blocage a dure environ {debloque:.0f} s"
      if debloque else "    -> toujours bloque apres 200 s")

# ------------------------------------------------------------------ C
print("\n[C] Quelle fenetre : 10 requetes par combien de secondes ?",
      flush=True)
# On vide, on attend F, on regarde si UNE seule requete repasse. La plus
# petite duree F qui laisse repasser, c'est la longueur de la fenetre.
for F in (15, 30, 45, 60, 90):
    repos(REPOS, "remise a zero")
    ok, code, cc = vider()
    time.sleep(F)
    c, d, cc = un()
    print(f"    apres {F:>3} s de silence, la requete suivante : {c}"
          f"   ({ok} etaient passees avant le refus)", flush=True)
    if c == 200:
        # combien en repassent d'affilee ? c'est la taille de la fenetre
        ok2, code2, cc2 = vider()
        print(f"        et {ok2 + 1} repassent d'affilee", flush=True)
        break

# ------------------------------------------------------------------ D
print("\n[D] Quel espacement tient sans jamais un seul refus ?", flush=True)
for d in (2.0, 3.0, 4.0, 6.0):
    repos(REPOS, "remise a zero")
    ok = ko = 0
    t0 = time.time()
    for i in range(24):
        c, dd, cc = un()
        if c == 200:
            ok += 1
        else:
            ko += 1
        reste = d - (time.time() - t0) % d
        time.sleep(max(0.0, min(reste, d)))
    duree = time.time() - t0
    print(f"    espacement {d:.1f} s : {ok} passees, {ko} refusees"
          f" en {duree:.0f} s -> {ok/duree*60:.1f} lues/min", flush=True)
    if ko == 0:
        print(f"        -> 142 ressources prendraient {142/(ok/duree)/60:.1f} min",
              flush=True)
        break

print("\n" + "=" * 64)
print(f"requetes totales de la sonde : {total[0]}")
print("=" * 64)
