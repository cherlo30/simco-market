"""Sonde : comprendre la limite du serveur au lieu de la supposer.

A lancer sur le runner GitHub, collecteur ARRETE — sinon les deux se
partagent le meme quota et la mesure ne veut rien dire.

Cinq experiences, dans l'ordre :
  0. ce que le serveur RACONTE de lui-meme (les en-tetes)
  1. combien de requetes passent d'affilee apres un long silence  -> capacite
  2. combien le silence en rend, selon sa duree                   -> recharge
  3. une requete refusee consomme-t-elle un jeton ?
  4. le cycle complet sur les 142 ressources, avec le meilleur reglage trouve
"""
import urllib.request, urllib.error, time, json, os

REALM = 0
BOOK = f"https://www.simcompanies.com/api/v3/market/all/{REALM}/%d/"
TICKER = f"https://www.simcompanies.com/api/v3/market-ticker/{REALM}/"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"

REPOS = float(os.environ.get("REPOS", "60"))   # silence avant chaque experience
tours = [0]


def un(kind, entetes=False):
    """Une requete. Renvoie (code, duree, en-tetes ou None)."""
    tours[0] += 1
    t = time.time()
    req = urllib.request.Request(BOOK % kind,
                                 headers={"User-Agent": UA,
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
            h = dict(r.headers) if entetes else None
        return 200, time.time() - t, h
    except urllib.error.HTTPError as e:
        h = dict(e.headers)
        try:
            e.read()
        except Exception:
            pass
        return e.code, time.time() - t, h
    except Exception as e:
        return -1, time.time() - t, {"erreur": str(e)[:80]}


def rafale(kinds, ecart=0.0, arret_au_refus=True, plafond=30):
    """Tire des requetes a la suite. Renvoie (passees, refusees, codes)."""
    ok = ko = 0
    codes = []
    for i in range(plafond):
        c, d, h = un(kinds[i % len(kinds)])
        codes.append(c)
        if c == 200:
            ok += 1
        else:
            ko += 1
            if arret_au_refus:
                break
        if ecart:
            time.sleep(ecart)
    return ok, ko, codes


def silence(s, quoi=""):
    print(f"    (silence de {s:.1f} s{' — ' + quoi if quoi else ''})", flush=True)
    time.sleep(s)


K = list(range(1, 40))          # les ressources servant de cobayes
print("=" * 62)
print("SONDE — comprendre la limite du serveur")
print("=" * 62, flush=True)

# ---------------------------------------------------------------- 0
print("\n[0] Ce que le serveur dit de lui-meme", flush=True)
silence(REPOS)
c, d, h = un(1, entetes=True)
print(f"    reponse {c} en {d*1000:.0f} ms")
interessants = [k for k in (h or {})
                if any(m in k.lower() for m in
                       ("rate", "limit", "retry", "remaining", "reset",
                        "quota", "throttle", "x-ratelimit"))]
if interessants:
    for k in interessants:
        print(f"    {k}: {h[k]}")
else:
    print("    aucun en-tete de quota annonce sur une reponse acceptee")
    print("    en-tetes vus : " + ", ".join(sorted(h or {})))

# ---------------------------------------------------------------- 1
print("\n[1] Capacite : combien passent d'affilee apres un long silence",
      flush=True)
capacites = []
for essai in (1, 2, 3):
    silence(REPOS, "on laisse la reserve se refaire completement")
    ok, ko, codes = rafale(K, ecart=0.0)
    capacites.append(ok)
    print(f"    essai {essai} : {ok} passees, puis {codes[-1]}", flush=True)
    if codes[-1] == 429:
        c2, d2, h2 = un(1)
        ra = (h2 or {}).get("Retry-After")
        if ra:
            print(f"    le serveur demande d'attendre {ra} s (Retry-After)")
CAP = min(capacites) if capacites else 0
print(f"    -> capacite retenue : {CAP} requetes")

# ---------------------------------------------------------------- 2
print("\n[2] Recharge : ce qu'un silence de T secondes rend", flush=True)
recharge = {}
for T in (1, 2, 3, 4, 6, 8, 12):
    silence(REPOS, "remise a zero")
    rafale(K, ecart=0.0)                 # on vide la reserve
    time.sleep(T)                        # on se tait T secondes
    ok, ko, codes = rafale(K, ecart=0.0)
    recharge[T] = ok
    print(f"    apres {T:>2} s de silence : {ok} requetes repassent", flush=True)

# ---------------------------------------------------------------- 3
print("\n[3] Une requete refusee consomme-t-elle un jeton ?", flush=True)
resultats = {}
for insistances in (0, 5):
    silence(REPOS, "remise a zero")
    rafale(K, ecart=0.0)                 # reserve vide
    for _ in range(insistances):         # on insiste pendant la penalite
        un(1)
        time.sleep(0.2)
    time.sleep(8)                        # meme silence dans les deux cas
    ok, ko, codes = rafale(K, ecart=0.0)
    resultats[insistances] = ok
    print(f"    {insistances} requete(s) pendant la penalite,"
          f" puis 8 s de silence -> {ok} repassent", flush=True)
print("    -> "
      + ("insister coute des jetons : la penalite se prolonge"
         if resultats.get(5, 99) < resultats.get(0, 0) - 1
         else "insister ne change rien de mesurable"))

# ---------------------------------------------------------------- 4
print("\n[4] Cycle complet sur toutes les ressources, avec ce qu'on vient"
      " d'apprendre", flush=True)
meilleur = max(recharge.items(), key=lambda kv: kv[1] / (kv[0] + 1e-9))
T_OPT, N_OPT = meilleur[0], meilleur[1]
print(f"    meilleur rendement mesure : {N_OPT} requetes pour {T_OPT} s"
      f" de silence, soit {N_OPT/T_OPT:.1f} requetes/s en regime", flush=True)

silence(REPOS, "remise a zero avant le vrai cycle")
tk = None
for _ in range(3):
    try:
        req = urllib.request.Request(TICKER, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            tk = json.loads(r.read().decode())
        break
    except Exception:
        time.sleep(5)
kinds = sorted({r["kind"] for r in tk}) if tk else list(range(1, 156))
print(f"    {len(kinds)} ressources a lire", flush=True)

restantes = list(kinds)
lues, refus = set(), 0
t0 = time.time()
passe = 0
while restantes and time.time() - t0 < 900:
    passe += 1
    encore = []
    i = 0
    while i < len(restantes):
        salve = restantes[i:i + N_OPT]
        for k in salve:
            c, d, h = un(k)
            if c == 200:
                lues.add(k)
            else:
                refus += 1
                encore.append(k)
        i += N_OPT
        if i < len(restantes):
            time.sleep(T_OPT)
    restantes = encore
    print(f"    passe {passe} : {len(lues)}/{len(kinds)} lues,"
          f" {refus} refus cumules, {(time.time()-t0)/60:.1f} min", flush=True)
    if restantes:
        time.sleep(T_OPT * 2)

duree = time.time() - t0
print("\n" + "=" * 62)
print("CONCLUSION")
print("=" * 62)
print(f"  capacite apres silence      : {CAP} requetes")
print("  recharge                    : "
      + " · ".join(f"{t} s -> {n}" for t, n in sorted(recharge.items())))
print(f"  reglage retenu              : salves de {N_OPT}, pause de {T_OPT} s")
print(f"  cycle complet               : {len(lues)}/{len(kinds)} ressources"
      f" en {duree/60:.1f} min, {refus} refus")
print(f"  requetes totales de la sonde : {tours[0]}")
