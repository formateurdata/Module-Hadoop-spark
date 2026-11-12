# Démo : blocs, copies et panne d'un DataNode

Les logs HTTP du serveur web du Kennedy Space Center (NASA), juillet et août 1995, sont déposés dans
HDFS avec des blocs de 32 Mo (la valeur par défaut est 128 Mo ; 32 Mo donne plusieurs blocs sur un
fichier de 195,7 Mo). On regarde où sont les blocs et leurs copies, on change la réplication, puis on
arrête un DataNode et on observe ce que fait le NameNode. Enfin on compare le même mois en un seul
fichier et en fichiers journaliers.

**Critères de réussite** (vérifiés par `run.py comparer`, qui sort en erreur si l'un échoue)

- Critère 1 : le nombre de blocs vaut ceil(taille / 32 Mo), et chaque bloc a 3 copies vivantes.
- Critère 2 : pendant la panne, le fichier relu a le même MD5 que l'original.
- Critère 3 : les blocs du fichier à 2 copies qui étaient sur le DataNode arrêté sont de nouveau à
  2 copies ; le fichier à 3 copies reste sous-répliqué (7 blocs) tant que le DataNode est absent, puis
  retombe à 0 à son retour.

Données : [NASA-HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html), Internet Traffic Archive. Deux
fichiers de 205 242 368 et 167 813 770 octets une fois décompressés, 37 Mo à télécharger. La page demande
de ne pas analyser les logs au-delà des tendances générales de trafic : la démo ne fait que les déposer.
La page annonce 31 jours pour juillet ; le fichier n'en contient que 28 (du 1er au 28 juillet), d'où les
28 fichiers journaliers de l'étape `petits_fichiers`.

## Installation

Le cluster doit tourner (voir le README à la racine du module). Cette démo suppose l'image reconstruite
avec la détection de panne accélérée (`dfs.namenode.heartbeat.recheck-interval` = 10 000 ms) :

```bash
cd <racine du module>
docker compose up -d --build
docker compose exec client hdfs dfsadmin -report                                          # Live datanodes (3)
docker compose exec client hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval  # 10000
```

Le script `run.py` tourne sur la machine hôte avec `uv` et n'a pas de dépendance. Il envoie les commandes
HDFS au conteneur client par `docker compose exec`, et arrête puis relance `worker1` par
`docker compose stop` et `start`.

## Commandes

Depuis ce dossier :

```bash
uv run python run.py telecharger   # facultatif : `tout` le fait si les données manquent ; télécharge et décompresse les deux mois, découpe juillet par jour (avant le cours)
uv run python run.py tout          # blocs, copies, panne, petits_fichiers, comparer : environ 3 min
```

| Étape | Ce qu'elle fait |
|---|---|
| `blocs` | repart d'un `/demo/nasa` vide, dépose juillet en blocs de 32 Mo, attend les copies, affiche les blocs |
| `copies` | passe le fichier à 2 copies puis à 3 copies, lit l'espace consommé (`hdfs dfs -du`) |
| `panne` | dépose juillet (3 copies) et août (2 copies), arrête `worker1`, attend qu'il soit déclaré mort, relit juillet (MD5), attend la re-réplication d'août, relance `worker1` |
| `petits_fichiers` | dépose juillet en un fichier, puis en fichiers journaliers (blocs de 128 Mo), compte les objets du NameNode |
| `comparer` | lit les résultats, affiche les critères et sort en erreur si l'un échoue |

Chaque étape repart d'un état vide et s'exécute à chaque lancement : rien n'est gardé d'une exécution à
l'autre, sauf les résultats écrits dans `resultats/`.

## Résultat de référence

Exécution du 07/11/2026, cluster neuf (`docker compose down -v` puis `up -d --build`), Mac à 10 cœurs,
16 Go alloués à Docker :

```
blocs : 7 blocs, 6 de 33 554 432 octets et 1 de 3 915 776, 3 copies chacun (worker1, worker2, worker3)
copies : 2 copies = 410 484 736 octets, 3 copies = 615 727 104 octets (= taille x facteur)
panne : worker1 déclaré mort après 53 s ; fichier d'août (2 copies) de nouveau complet après 61 s ;
        juillet (3 copies) : 7 blocs sous-répliqués, MD5 4138cd7e981746fa08de662228b567c2 identique ;
        à son retour, juillet est de nouveau complet 4 s après
petits fichiers : 1 fichier = 1 + 2 = 3 objets ; 28 fichiers = 28 + 28 = 56 objets ; espace identique
```

Ce qui varie d'une exécution à l'autre : les DataNodes choisis pour chaque bloc, donc le nombre de blocs
d'août qui étaient sur `worker1` (de 3 à 6 sur les essais de préparation), le délai de détection (48 à
53 s) et les temps de dépôt (1,6 à 3,2 s).

## Où regarder dans l'interface

NameNode, http://localhost:9870 :

| Où | Ce qu'on y voit |
|---|---|
| Overview, Summary | « 41 files and directories, 30 blocks = 71 total filesystem object(s) » après `petits_fichiers` ; « Safemode is off » ; Live Nodes, Dead Nodes ; Number of Under-Replicated Blocks (7 pendant la panne de l'étape `panne`) |
| Overview | Last Checkpoint Time (dans Summary) ; plus bas, NameNode Journal Status : le fichier de journal en cours (`edits_inprogress`) |
| Datanodes | les trois DataNodes dans `/default-rack` ; colonne Blocks ; pendant la panne, `worker1` sur fond rouge, avec la date de son dernier contact |
| Utilities > Browse the file system | `/demo/nasa/jours` : pour chaque fichier, les colonnes Replication et Block Size |

Les captures des slides sont produites par `slides/build/captures_seance2.py`, qui reconstitue l'état de
l'étape `panne`, arrête `worker1` et le relance.

## Dépannage

- **`put: ... data/NASA_access_log_Jul95: No such file or directory`** : les données n'étaient pas téléchargées. La version actuelle de `run.py` les télécharge toute seule ; sinon `uv run python run.py telecharger`.
- **`worker1` reste arrêté après une interruption** (Ctrl-C pendant l'étape `panne`) :
  `docker compose start worker1`, puis attendez 3 DataNodes vivants. Le script relance `worker1` dans
  un bloc `finally`, mais pas si le processus est tué.
- **Le fichier « à 2 copies » n'a aucune re-réplication à montrer** : `worker1` était arrêté quand le
  fichier a été déposé, donc aucun de ses blocs n'est sur `worker1` (rencontré pendant la préparation).
  Le script relance les trois DataNodes et attend qu'ils soient vivants avant chaque dépôt.
- **Un bloc n'a que 2 copies sur 3 juste après le dépôt** (rencontré pendant la préparation) : le dépôt
  rend la main avant que la dernière copie soit finie. Le script attend la fin de la réplication avant
  de lire les blocs (`attendre_copies`) ; à la main, relancez `fsck` quelques secondes plus tard.
- **`Délai dépassé (180 s) : re-réplication du fichier à 2 copies`** (rencontré pendant la préparation) :
  le fichier avait été déposé avec 3 copies parce que l'option `-D dfs.replication=2` était mal écrite :
  HDFS ignore une propriété qu'il ne connaît pas, sans erreur. Vérifiez la commande avec
  `hdfs dfs -stat "%r" <fichier>` (attendu : 2).
- **`AttributeError: 'NoneType' object has no attribute 'group'`** dans `adresses` (rencontré pendant
  la préparation) : le rapport du NameNode n'affiche plus le nom d'un DataNode mort. La version actuelle
  mémorise la table adresse-nom avant la panne.
- **Le DataNode n'est déclaré mort qu'après dix minutes** : l'image n'a pas été reconstruite. Lancez
  `docker compose up -d --build` et vérifiez
  `hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval` (attendu : 10000).
- **`put: File exists`** quand vous rejouez une commande à la main : ajoutez `-f`, ou supprimez
  d'abord la cible avec `hdfs dfs -rm -r -skipTrash`. Le script, lui, repart toujours d'un `/demo/nasa`
  vide. Attention : sur ce cluster la corbeille est désactivée, `rm` est définitif.
- **`Bind for 127.0.0.1:8080 failed: port is already allocated`** : un autre programme utilise ce port ;
  voir le dépannage du README à la racine.
