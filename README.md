# simco-market

Releve en continu le marche de Sim Companies (royaume 0) et publie un
tableau de bord.

**Tableau de bord :** https://cherlo30.github.io/simco-market/

## Comment ca marche

`collect.py` tourne en boucle sur GitHub Actions. Il interroge les
~143 ressources une par une (un tour complet dure ~3 min), compare ce
qu'il voit au carnet enregistre, en deduit ce qui s'est vendu, et
reecrit le carnet.

**Le programme ne se souvient de rien : sa memoire est le fichier.**
Au demarrage il lit la branche `live`. Quand GitHub coupe la course au
bout de 5 h 30, le programme suivant reprend exactement ou l'autre
s'est arrete. Aucun trou.

## Les fichiers

Branche `live` — reecrite par-dessus elle-meme a chaque envoi
(5 min). GitHub n'en garde qu'un exemplaire, aucun historique :

| fichier | contenu |
|---|---|
| `ordres.csv` | toutes les offres en vente, avec leur variation |
| `heure_horaire.csv` | l'heure en cours, en construction |
| `heure_volume.csv` | les volumes par prix de l'heure en cours |

Branche `main` — ne recoit qu'une heure **terminee**, ajoutee une fois
pour toutes :

| fichier | contenu |
|---|---|
| `data/horaire/AAAA-MM.csv` | par heure et par ressource : ouverture, haut, bas, cloture, profondeur, vendu, retire |
| `data/volume/<ressource>/AAAA-MM.csv` | volume echange a chaque palier de prix |

Environ 300 Ko par jour.

## Vendu ou retire

Le jeu ne permet pas de reduire une offre en vente : **toute baisse
partielle est une vente.**

Seule la disparition complete d'une offre est ambigue. On tranche par
le prix : si l'offre disparue etait au meilleur prix ou en dessous,
personne n'aurait achete ailleurs, c'est une **vente** ; s'il restait
moins cher sur le marche, c'est un **retrait** de son vendeur.

## La chaine

`loop.yml` collecte 5 h 30, puis se relance lui-meme via le secret
`PAT` (le jeton `GITHUB_TOKEN` n'a pas le droit de declencher un
workflow). Un `cron` toutes les 3 h sert de filet si la chaine casse.
