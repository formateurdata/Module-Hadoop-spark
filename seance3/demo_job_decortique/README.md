# Démo : un job MapReduce décortiqué

Le notebook `demo_job_decortique.ipynb` agrège les logs HTTP du serveur web du Kennedy Space Center
(NASA), juillet 1995, par code de réponse HTTP, puis par hôte : nombre de requêtes et octets envoyés. Le
même mapper et le même reducer, en Python, tournent d'abord sur une machine, reliés par un pipe, puis sur
le cluster avec Hadoop Streaming : sans combiner, avec combiner, et avec une tâche map qu'on fait échouer
en plein job. Le fichier est déposé dans HDFS en blocs de 32 Mo (7 blocs, donc 7 tâches map), et les jobs
ont 3 reducers.

Chaque cellule fait une seule chose. Une cellule qui commence par `%%client` contient des commandes bash,
exécutées dans le conteneur client du cluster ; le mapper et le reducer sont écrits par deux cellules
`%%writefile`.

**Critère de réussite** : chaque job du cluster donne exactement le résultat du même calcul en Python sur
une seule machine (mêmes clés, mêmes nombres de requêtes, mêmes octets). La dernière cellule le vérifie et
s'arrête en erreur sinon. Les sorties des cellules montrent aussi :

- le job lit toutes les lignes du fichier (`Map input records`) et n'en ignore qu'une, la dernière,
  tronquée (`Lignes ignorees`) ;
- avec une tentative de tâche map mise en échec, le job réussit quand même (`Failed map tasks=1`,
  `Launched map tasks=8` pour 7 splits) ;
- le combiner réduit les octets envoyés par le shuffle (`Reduce shuffle bytes`).

Données : [NASA-HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html), Internet Traffic Archive.
Un fichier de 205 242 368 octets une fois décompressé (`data/jul95.log`), 21 Mo à télécharger,
1 891 715 lignes ; la dernière est tronquée et n'a pas de fin de ligne. La page demande de ne pas
analyser les logs au-delà des tendances générales de trafic : les slides ne montrent ni nom d'hôte ni
classement des hôtes, seulement leur nombre (81 982) et les volumes du shuffle.

## Installation

Le cluster doit tourner (voir le README à la racine du module), avec le serveur d'historique MapReduce
sur http://localhost:19888. Cette démo suppose l'image reconstruite avec le correctif de
`cluster/demarrer.sh` :

```bash
cd <racine du module>
docker compose up -d --build
docker compose exec client hdfs dfsadmin -report | grep "Live datanodes"   # Live datanodes (3)
curl -s http://localhost:19888/ws/v1/history/info                         # répond, environ une minute après le démarrage
uv sync --group tp                                                        # noyau .venv du notebook
```

Ouvrez le notebook depuis son dossier, `seance3/demo_job_decortique`, avec le noyau `.venv` du module.
Il n'utilise que la bibliothèque standard de Python et IPython : `docker compose exec` lui sert à
joindre le conteneur client, où le dossier du module est monté sous `/cours`. Le mapper et le reducer
tournent dans les conteneurs, sous Python 3.10.

## Exécution

En cours, cellule par cellule. Pour tout exécuter d'un coup (environ 3 min), « Exécuter tout » dans
Jupyter ou VS Code, ou, depuis ce dossier :

```bash
uv run --group tp --with nbconvert==7.16.6 jupyter nbconvert --to notebook --execute --inplace demo_job_decortique.ipynb
```

| Partie du notebook | Ce qu'elle fait |
|---|---|
| Préparation | définit `%%client` ; télécharge juillet 1995 dans `data/jul95.log`, à chaque exécution ; repart d'un `/demo3` vide, y dépose juillet en blocs de 32 Mo, et ses 10 premières lignes dans `/demo3/petit` |
| Le mapper et le reducer | écrit `mapper.py` et `reducer.py` dans ce dossier (`%%writefile`) |
| 1. Sur une machine | les lignes 48 à 53 (celles de l'exercice 1) passent dans le mapper, `LC_ALL=C sort`, puis le reducer : une cellule par étape |
| 2. Sur le cluster | le job par code HTTP, avec son journal complet (progression, compteurs), puis le contenu de chaque fichier `part-*` |
| 3. Avec un combiner | par code avec combiner, par hôte sans puis avec combiner ; `grep` ne garde que les compteurs utiles |
| 4. Une tâche échoue | le job par code tourne en arrière-plan ; dès qu'il démarre, `mapred job -fail-task` fait échouer la tentative `attempt_<numéro du job>_m_000001_0` |
| Le coût fixe d'un job | le même job sur les 10 premières lignes |
| Le critère de réussite | rapatrie le résultat des cinq jobs dans `data/`, refait le calcul en Python et compare |

Chaque job efface sa sortie avant de démarrer : une cellule peut être relancée seule. `%%time` donne la
durée d'une cellule de job, de bout en bout (`Wall time`).

## Résultat de référence

Exécution du 18/11/2026, Mac Apple Silicon à 10 cœurs, Docker 29.9, jobs `job_1795021959367_0037` à
`0042` :

```
dépôt : 205 242 368 octets, 7 blocs, réplication moyenne 3,0
une machine (lignes 48 à 53) : map 302 1 110 | 200 1 7634 | 200 1 3985 | 404 1 0 | 200 1 7634 | 200 1 55666
                               reduce 200 4 74919 | 302 1 110 | 404 1 0
job par code : Map input records 1 891 715, Map output records 1 891 714, Lignes ignorees 1,
               Launched map tasks 7 (7 data-local), Killed map tasks 1, Shuffled Maps 21,
               Reduce input groups 8, Reduce shuffle bytes 24 254 418 ; 18,2 s
               part-00000 : 200, 302, 404, 500 ; part-00001 : 501 ; part-00002 : 304, 400, 403
combiner : par code, 1 891 714 -> 45 enregistrements, shuffle 778 octets (divisé par 31 175,3) ; 18,4 s
           par hôte, 81 982 clés : shuffle 53 556 194 octets sans combiner (21,5 s),
           3 414 865 avec (divisé par 15,7), 1 891 714 -> 111 812 enregistrements (20,6 s)
panne : attempt_1795021959367_0041_m_000001_0 FAILED sur worker2 (2,9 s),
        attempt_1795021959367_0041_m_000001_1 SUCCEEDED sur worker3 (1,8 s) ;
        Failed map tasks 1, Launched map tasks 8 ; 21,3 s
coût fixe : 10 lignes, 2 splits, 16,8 s (juillet entier : 18,2 s)
critère : les cinq jobs identiques au calcul Python (8 codes, 81 982 hôtes)
```

`Killed map tasks 1` sur le job par code vient de l'exécution spéculative : le journal de
l'ApplicationMaster montre une copie de secours de la tâche `m_000003`, tuée 15 ms après, quand l'original
a fini.

Ce qui varie d'une exécution à l'autre : les durées (de 16 à 22 s par job sur cette machine, jusqu'à 100 s
quand la machine était chargée pendant la préparation), les nœuds des tentatives, et l'exécution
spéculative (`Killed map tasks` vaut 0 ou 1 selon les exécutions, et une copie de secours peut aussi viser
un reducer). Les compteurs d'enregistrements et d'octets, les fichiers `part-*` et leur contenu ne varient
pas.

## Où regarder dans l'interface

Historique MapReduce, http://localhost:19888. Les jobs Streaming s'y appellent `streamjob<nombre>.jar` :
repérez-les par leur identifiant, affiché par chaque cellule de job (`Running job: job_...`).

| Où | Ce qu'on y voit |
|---|---|
| Jobs (page d'accueil) | les jobs du notebook, leur état et le nombre de tâches map et reduce |
| un job, puis Counters (menu de gauche) | les groupes de compteurs : `Map input records`, `Map output records`, `Reduce shuffle bytes`, `Shuffled Maps` (21 dans la colonne Reduce), `Combine input records` ; le groupe `Demo` avec `Lignes ignorees` = 1, notre compteur, incrémenté par le mapper sur sa sortie d'erreur |
| le job de la panne, Map tasks, puis la tâche `m_000001` | ses deux tentatives : l'une FAILED, avec la note « Fail task attempt ... received from spark », l'autre SUCCEEDED, avec le nœud (`/default-rack/worker...`) et la durée |

ResourceManager, http://localhost:8088 : chaque job y apparaît comme une application YARN de type
MAPREDUCE pendant qu'il tourne.

Le journal de l'ApplicationMaster, pour l'exécution spéculative, se lit dans un terminal (remplacez le
numéro par celui du job) :

```bash
docker compose exec client bash -c "yarn logs -applicationId application_1795021959367_0037 | grep -E 'we are speculating|Issuing kill'"
```

## Dépannage

- **`UnicodeDecodeError: 'utf-8' codec can't decode byte 0x80`** dans le journal d'une tentative map, et le
  job échoue après 4 tentatives (rencontré pendant la préparation) : 8 lignes de juillet contiennent des
  octets qui ne sont pas de l'UTF-8, et Python lit l'entrée standard en UTF-8 dans les conteneurs. Le
  mapper lit l'entrée en latin-1 : `sys.stdin.reconfigure(encoding="latin-1")`.
- **Le test par un pipe donne plus de lignes que de clés** (rencontré pendant la préparation : 82 259
  lignes pour 81 982 hôtes) : `sort` trie selon la langue du conteneur (`en_US.UTF-8`), qui ignore une
  partie de la ponctuation ; des lignes d'un même hôte ne sont plus voisines, et le reducer coupe le
  groupe en deux. Triez octet par octet, comme Hadoop : `LC_ALL=C sort`.
- **La page http://localhost:19888 ne répond pas** (rencontré pendant la préparation : `Connection
  refused`) : le serveur d'historique n'avait pas démarré. L'ancien `cluster/demarrer.sh` demandait l'état
  du safe mode avant que le NameNode écoute, recevait `Connection refused` et ne lançait jamais
  l'historique. Reconstruisez l'image (`docker compose up -d --build`) et attendez environ une minute.
- **La panne ne prend pas** (rencontré pendant la préparation) : `-fail-task` répond « Killed task ... by
  failing it », mais le job affiche `Launched map tasks=7` et aucune ligne `Failed map tasks`. La première
  version de la cellule cherchait une tentative en cours avec `mapred job -list-attempt-ids`, puis la
  faisait échouer ; chaque appel à `mapred` prend 1 à 3 s, et la tentative avait déjà réussi quand
  l'ordre est arrivé. La cellule actuelle vise la tentative `_m_000001_0` dès que le job démarre, avant
  qu'elle ait fini. Si cela se reproduit, relancez la cellule.
- **`java.io.IOException: Invalid operation on completed job`** (rencontré pendant la préparation) :
  `mapred job -fail-task` sur un job déjà terminé. Relancez la cellule de la panne en entier : elle relance
  le job.
- **`ERROR streaming.StreamJob: Unrecognized option: -D`**, puis `Streaming Command Failed!` (rencontré
  pendant la préparation, en déplaçant une option) : les options génériques (`-D ...`) doivent précéder
  les options de streaming (`-files`, `-mapper`, `-input`...).
- **`Output directory ... already exists`** quand vous rejouez une commande de job à la main : la sortie
  d'un job ne doit pas exister. Supprimez-la d'abord, comme le fait la première ligne de chaque cellule :
  `hdfs dfs -rm -r -f -skipTrash /demo3/sorties/code`. Sur ce cluster, la corbeille est désactivée : `rm`
  est définitif.
- **Les jobs sont beaucoup plus lents que dans le résultat de référence** (rencontré pendant la
  préparation : 100 s au lieu de 20 s de bout en bout, puis Docker Desktop s'est arrêté) : la machine
  était chargée par d'autres programmes. Fermez-les, relancez Docker Desktop et le cluster
  (`docker compose up -d`), puis le notebook. Les compteurs ne dépendent pas de la charge, seules les
  durées changent.
