# Démo : de la transformation à l'action

Le notebook `demo_transformation_action.ipynb` travaille sur les trajets des taxis jaunes de New York de
janvier 2024, publiés par la Taxi and Limousine Commission (TLC). Il lit un fichier Parquet, écrit des
transformations, lance des actions, lit un fichier CSV et écrit un résultat en Parquet dans HDFS. Pendant la
démo, on suit le travail du cluster dans la Spark UI.

Chaque cellule fait une seule chose. Une cellule qui commence par `%%client` contient des commandes bash,
exécutées dans le conteneur client du cluster, où est installé `hdfs`. Les autres tournent sur votre machine
et envoient leurs DataFrames au serveur Spark Connect du cluster (port 15002).

**Critère de réussite** : les transformations s'écrivent sans aucun calcul ; le nombre de trajets, total et
par type de paiement, calculé par Spark sur le cluster, est identique au calcul de pandas sur le même
fichier. La dernière cellule le vérifie et s'arrête en erreur sinon.

## Données

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) :
  `yellow_tripdata_2024-01.parquet`, 49 961 641 octets, 2 964 624 trajets, 19 colonnes.
- Table des zones de taxi : `taxi_zone_lookup.csv`, 12 331 octets, 265 zones.

Le notebook télécharge les deux fichiers dans `data/` s'ils sont absents. Les codes de `payment_type` sont
dans le [dictionnaire des données](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) :
0 Flex Fare, 1 carte, 2 espèces, 3 sans frais, 4 litige.

## Installation

Le cluster doit tourner (voir le README à la racine du module) :

```bash
cd <racine du module>
docker compose up -d
uv sync --group tp          # noyau .venv : pyspark-client 4.1.3, pandas, pyarrow
```

Ouvrez le notebook depuis son dossier, `seance4/demo_transformation_action`, avec le noyau `.venv` du
module.

## Exécution

En cours, cellule par cellule. Pour tout exécuter d'un coup (moins d'une minute), « Exécuter tout » dans
Jupyter ou VS Code, ou, depuis ce dossier :

```bash
uv run --group tp --with nbconvert==7.16.6 jupyter nbconvert --to notebook --execute --inplace demo_transformation_action.ipynb
```

| Partie du notebook | Ce qu'elle fait |
|---|---|
| Préparation | définit `%%client` ; télécharge les deux fichiers ; les dépose dans un dossier `/demo4` vide de HDFS |
| 1. La session | se connecte au serveur Spark Connect |
| 2. Lire un fichier Parquet | lit janvier et affiche le schéma écrit dans le fichier |
| 3. Des transformations | un filtre et une sélection : rien n'est calculé |
| 4. Une action | `count()` lance le calcul, puis `show(5)` |
| 5. Une deuxième action | la somme des montants : Spark relit le fichier |
| 6. Regrouper | les trajets par type de paiement |
| 7. Lire un fichier CSV | sans option, puis avec `header` et `inferSchema` |
| 8. Écrire en Parquet | le résultat de la partie 6 dans `/demo4/par_paiement` |
| Le critère de réussite | pandas lit le même fichier, applique le même filtre et compare |

## Résultat de référence

Exécution du 26/11/2026 (Spark 4.1.3, Mac Apple Silicon à 10 cœurs, Docker 29.9) :

```
3. transformations : Plan enregistré, aucun calcul ; 1,92 ms
4. count() : 2872021 trajets retenus ; 373 ms
5. somme des montants : 78488879 dollars ; 225 ms
6. par type de paiement : 0 117329, 1 2298408, 2 422863, 3 10615, 4 22806
7. CSV sans option : _c0 à _c3, string ; avec header et inferSchema : LocationID integer
8. /demo4/par_paiement : _SUCCESS et un fichier part-00000-...snappy.parquet de 808 octets
critère : atteint
```

Les durées varient d'une exécution à l'autre. Le premier `count()` après le démarrage du cluster attend le
lancement des executors et prend plusieurs secondes.

## Où regarder dans l'interface

Spark UI du serveur Spark Connect, http://localhost:4040, onglet **Jobs**. Les descriptions des jobs
affichent l'identifiant de session et le nom d'utilisateur : masquez la colonne Description avant de
projeter.

| Après la partie | Dans l'onglet Jobs (comptes vérifiés le 26/11/2026) |
|---|---|
| 2. Lire un fichier Parquet | 1 job : Spark lit le schéma dans le fichier |
| 3. Des transformations | aucun nouveau job |
| 4. Une action | 2 nouveaux jobs pour `count()` |
| 5. Une deuxième action | 2 nouveaux jobs : le calcul est refait |

Master Spark, http://localhost:8080 : les 3 workers, 6 cœurs au total, et l'application `spark-connect`.

## Dépannage

- **`rm: cannot remove '/tmp/...': Operation not permitted`** (rencontré pendant la préparation) : les
  fichiers copiés par `docker compose cp` appartiennent à l'utilisateur de votre machine, et l'utilisateur du
  conteneur ne peut pas les supprimer de `/tmp`. Le notebook ne les supprime pas : la copie suivante les
  écrase. Pour les retirer : `docker compose exec -u root client rm /tmp/yellow_tripdata_2024-01.parquet
  /tmp/taxi_zone_lookup.csv`.
- **Une ligne `FD from fork parent still in poll list` sous une cellule `%%client`** (rencontré pendant la
  préparation) : message de la bibliothèque gRPC du client Spark Connect quand le notebook lance `docker`. La
  première cellule le fait taire avec `os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "false"` ; redémarrez le
  noyau si la session Spark a été ouverte avant.
- **Le premier `count()` prend plusieurs secondes** : aucun executor n'était lancé, le premier job attend le
  sien. Les executors restent 60 s après la dernière tâche (`cluster/demarrer.sh`).
- **La Spark UI a changé d'application** : le conteneur client a redémarré, et le serveur Spark Connect avec
  lui ; son interface repart de zéro. Relancez le notebook depuis la partie 1.
