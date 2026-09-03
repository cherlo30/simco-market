# simco-market — collecteur de prix Sim Companies

Enregistre l'historique complet du marche (realm 0), que le jeu n'expose nulle part.
Tourne tout seul sur GitHub Actions, gratuitement, sans serveur ni ordinateur allume.

## Ce qui est collecte

| Fichier | Frequence | Contenu |
|---|---|---|
| `data/ticker/AAAA-MM-JJ.csv` | 10 min | prix des ~140 ressources + tendance |
| `data/book/AAAA-MM-JJ.csv` | 1 h | meilleure offre, profondeur, age median des ordres |

`median_age_h` est la mesure directe de la vitesse d'ecoulement : l'age median
des 20 offres les moins cheres. En dessous de 2 h le produit part vite, au-dela
de 20 h il stagne.

## Installation — 5 minutes

1. Sur github.com : **New repository**, nom `simco-market`, visibilite **Public**
   (les depots publics ont des minutes d'execution illimitees), coche
   *Add a README file*, puis **Create repository**.

2. **Add file > Upload files**. Glisse tout le contenu de ce dossier
   (`collect_ticker.py`, `collect_books.py`, `analyse.py`, `README.md`,
   les dossiers `.github` et `data`). Ecris un message et **Commit changes**.

   > Si le dossier `.github` ne monte pas par glisser-deposer, utilise
   > **Add file > Create new file**, tape `.github/workflows/ticker.yml`
   > comme nom, colle le contenu, valide. Recommence pour `books.yml`.

3. Onglet **Actions**. GitHub demande une confirmation la premiere fois :
   **I understand my workflows, go ahead and enable them**.

4. Clique le workflow **ticker** > **Run workflow** pour verifier tout de suite.
   Un premier fichier doit apparaitre dans `data/ticker/`.

C'est fini. La collecte tourne ensuite toute seule.

## Analyse

Telecharge le depot (**Code > Download ZIP**), puis :

```
python3 analyse.py 146          # citrouille
python3 analyse.py 146 2 66 13  # citrouille, eau, semences, transport
```

Sortie : profil horaire en UTC (a quelle heure ca monte, a quelle heure ca creuse),
amplitude par jour, et le prix fixe conseille pour un contrat, avec son
equivalent en vente a la bourse.

## Notes

- Les deux endpoints utilises sont publics, aucune authentification.
- Les taches planifiees de GitHub peuvent glisser de 5 a 20 minutes en periode
  chargee : l'horodatage inscrit dans le CSV est l'heure reelle du releve, pas
  l'heure theorique. Rien a corriger.
- Le balayage des carnets dure ~3 minutes (140 requetes espacees de 0,8 s).
- Pour suivre un autre realm, change `REALM` en haut des deux scripts.
