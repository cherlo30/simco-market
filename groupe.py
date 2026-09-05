"""Une seule question, six requetes, une minute :
   une requete peut-elle ramener plusieurs produits ?

Le budget du serveur se compte en REQUETES (une dizaine par minute), pas en
produits. Si un appel groupe existe, le cycle est divise d'autant — et le
serveur recoit moins de trafic qu'aujourd'hui, pas plus.
"""
import urllib.request, urllib.error, time, json

REALM = 0
BASE = "https://www.simcompanies.com"
UA = "Mozilla/5.0 (compatible; simco-market-logger/3.0)"


def get(chemin):
    req = urllib.request.Request(BASE + chemin,
                                 headers={"User-Agent": UA,
                                          "Accept": "application/json"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return 200, r.read(), time.time() - t, dict(r.headers)
    except urllib.error.HTTPError as e:
        try:
            e.read()
        except Exception:
            pass
        return e.code, b"", time.time() - t, dict(e.headers)
    except Exception as e:
        return -1, str(e).encode()[:80], time.time() - t, {}


def decrire(brut):
    """Combien de produits distincts cette reponse contient-elle ?"""
    try:
        d = json.loads(brut.decode())
    except Exception:
        return "reponse non-JSON", 0, None
    if isinstance(d, dict):
        return f"objet a {len(d)} cles", 0, d
    kinds = {e["kind"] for e in d
             if isinstance(e, dict) and "kind" in e}
    return f"liste de {len(d)} entrees", len(kinds), d


print("=" * 66)
print("Une requete peut-elle ramener plusieurs produits ?")
print("=" * 66, flush=True)

# ------------------------------------------------------------------ 1
print("\n[1] market-ticker : que contient-il exactement ?", flush=True)
c, brut, d, h = get(f"/api/v3/market-ticker/{REALM}/")
if c == 200:
    forme, nk, data = decrire(brut)
    print(f"    200 en {d*1000:.0f} ms, {len(brut)//1024} Ko — {forme},"
          f" {nk} produits distincts")
    if data:
        print("    champs : " + ", ".join(sorted(data[0])))
        for e in data[:3]:
            print("      " + json.dumps(e, ensure_ascii=False)[:220])
        prix = [k for k in data[0] if any(m in k.lower() for m in
                ("price", "prix", "cost", "value", "bid", "ask", "amount"))]
        print("    -> " + (f"CHAMPS DE PRIX : {', '.join(prix)}"
                           " — une requete donne tous les prix du marche"
                           if prix else
                           "aucun champ de prix : le ticker ne sert qu'a lister"
                           " les produits"))
else:
    print(f"    {c}")
time.sleep(7)

# ------------------------------------------------------------------ 2
print("\n[2] Les formes d'URL groupees", flush=True)
reference = None
FORMES = [
    ("un seul produit (reference)",   f"/api/v3/market/all/{REALM}/1/"),
    ("sans preciser le produit",      f"/api/v3/market/all/{REALM}/"),
    ("produits separes par virgule",  f"/api/v3/market/all/{REALM}/1,2,3/"),
    ("liste en parametre",            f"/api/v3/market/all/{REALM}/1/?kinds=1,2,3"),
    ("marche sans le mot all",        f"/api/v3/market/{REALM}/"),
]
gagnantes = []
for nom, chemin in FORMES:
    c, brut, d, h = get(chemin)
    if c == 200:
        forme, nk, _ = decrire(brut)
        print(f"    200  {nom:<31} {len(brut)//1024:>5} Ko  {forme},"
              f" {nk} produit(s)", flush=True)
        if reference is None:
            reference = nk
        elif nk > max(1, reference):
            gagnantes.append((nom, chemin, nk))
    else:
        print(f"    {c:>3}  {nom:<31} refuse", flush=True)
    time.sleep(7)          # on reste sous les ~10 requetes par minute

print("\n" + "=" * 66)
if gagnantes:
    n, ch, nk = max(gagnantes, key=lambda x: x[2])
    print(f"TROUVE : « {n} » ramene {nk} produits en une requete.")
    print(f"  {ch}")
    print(f"  -> les 142 produits en {-(-142 // max(nk,1))} requetes,"
          f" soit environ {-(-142 // max(nk,1)) / 10:.1f} min de cycle")
else:
    print("Aucune forme groupee ne repond : une requete = un produit.")
    print("  -> il reste la priorisation, et la demande d'un acces groupe")
    print("     aux developpeurs du jeu.")
print("=" * 66)
