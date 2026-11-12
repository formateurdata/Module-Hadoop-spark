# Big Data (Hadoop & Spark)

Module Bac+5 Data Analytics et Big Data, Institut GoldenCollar. Enseignant : Mouaad AGOURRAM.

Le module tourne sur un cluster Hadoop et Spark lancé avec Docker Compose sur la machine de
chaque étudiant : cinq conteneurs, chacun dans le rôle d'une machine du cluster.

| Conteneur | Rôle | Interfaces |
|---|---|---|
| `master` | NameNode (HDFS), ResourceManager (YARN), historique MapReduce, master Spark | 9870, 8088, 19888, 8080 |
| `worker1` à `worker3` | DataNode, NodeManager, worker Spark (2 cœurs, 2 Go chacun) | aucune |
| `client` | Serveur Spark Connect, commandes `hdfs`, `mapred`, `spark-submit` ; les drivers Spark y tournent | 15002, 4040 |

Une seule image, construite par `cluster/Dockerfile` : Spark 4.1.3 (image officielle
`apache/spark`, Java 17, Python 3.10) et Hadoop 3.5.0, dont l'archive est vérifiée par sa somme
SHA-512. La configuration Hadoop est dans `cluster/conf/hadoop/`, celle de Spark dans
`cluster/conf/spark/`.

## Prérequis

- Docker Desktop (ou Docker Engine avec Compose v2). Mémoire allouée à Docker : 8 Go conseillés.
  Pendant la démo de la séance 1, les cinq conteneurs ont occupé au plus 5,2 Go au total.
- Disque : l'image fait 4,1 Go ; la première construction télécharge environ 1,4 Go (image de
  base de Spark et archive de Hadoop).
- [uv](https://docs.astral.sh/uv/) pour les notebooks des TP, les scripts des démos et la génération
  des slides. Java n'est pas nécessaire sur la machine : il ne tourne que dans les conteneurs.
- Mac Apple Silicon ou Intel, Linux, Windows avec WSL 2 : le Dockerfile choisit l'archive de Hadoop
  adaptée au processeur (`aarch64` ou `x86_64`).

## Commandes

Depuis ce dossier :

```bash
docker compose up -d --build                       # construit l'image (la première fois) et démarre
docker compose exec client hdfs dfsadmin -report   # attendu : Live datanodes (3)
docker compose exec client yarn node -list         # attendu : Total Nodes:3
docker compose ps                                  # état des cinq conteneurs
docker compose logs -f master                      # journaux d'un nœud

docker compose stop                                # arrête, garde les données HDFS
docker compose start                               # redémarre
docker compose down                                # supprime les conteneurs, garde les volumes
docker compose down -v                             # supprime aussi les volumes : HDFS repart vide
```

## Détection de panne accélérée

Pour la démo de la séance 2, `cluster/conf/hadoop/hdfs-site.xml` ramène `dfs.namenode.heartbeat.recheck-interval`
à 10 000 ms : un DataNode silencieux est déclaré mort après 50 s (2 x 10 s + 10 x 3 s) au lieu de 630 s par
défaut. Ce réglage est dans l'image : après avoir récupéré cette version, relancez
`docker compose up -d --build`, puis vérifiez avec
`docker compose exec client hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval` (attendu : 10000).
Ce n'est pas un réglage de production.

## Notebooks des TP

Les notebooks tournent sur votre machine, dans votre environnement Python, hors du cluster :

- Spark passe par **Spark Connect** : le serveur tourne dans le conteneur `client`, le notebook
  n'embarque que le client Python `pyspark-client`, sans Java ;
- les commandes Hadoop (`hdfs`, `mapred`) passent par le conteneur `client`, avec
  `docker compose exec` ; chaque notebook fournit une fonction `cluster(commande)` pour cela ;
- le dossier du module est monté dans le conteneur `client` sous `/cours`.

```bash
uv sync --group tp      # depuis ce dossier : crée .venv avec pyspark-client et ipykernel
```

Ouvrez ensuite le notebook depuis son dossier (`seanceN/tp/`), dans Jupyter ou VS Code, avec
`.venv` comme noyau. Dans un autre environnement Python, installez `pyspark-client==4.1.3`. La
session Spark se crée ainsi :

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

Avec Spark Connect, les DataFrames et Spark SQL sont disponibles, mais pas l'API RDD ni
`sparkContext`. Le serveur rend ses executors au master après une à deux minutes sans requête :
la première requête qui suit prend quelques secondes de plus.

## Interfaces

Les ports sont publiés sur `127.0.0.1` seulement : Spark Connect n'a pas d'authentification, et le
cluster ne doit pas être joignable depuis le réseau de l'école.

| Adresse | Ce qu'on y voit |
|---|---|
| http://localhost:9870 | NameNode : capacité, DataNodes vivants, explorateur de fichiers (Utilities > Browse the file system) |
| http://localhost:8088 | ResourceManager : applications YARN, nœuds, ressources |
| http://localhost:19888 | Historique MapReduce : jobs terminés, compteurs, tâches |
| http://localhost:8080 | Master Spark : workers, applications en cours et terminées |
| http://localhost:4040 | Serveur Spark Connect : onglet Connect (sessions et requêtes des notebooks), jobs, SQL |

## Organisation

```
docker-compose.yml        le cluster
cluster/                  image et configuration
pyproject.toml            versions épinglées des outils (uv)
seanceN/demo_<sujet>/     démo de la séance, avec son README
seanceN/tp/               énoncé et correction du TP (notebooks, exécutés sur votre machine)
slides/                   présentations et leur générateur, voir slides/README.md
syllabus/                 syllabus du module
```

## Dépannage

- **`Bind for 127.0.0.1:8080 failed: port is already allocated`** (ou 15002, 9870...) : un autre
  programme utilise ce port. Arrêtez-le, ou changez le port côté machine dans
  `docker-compose.yml`, par exemple `"127.0.0.1:18080:8080"`.
- **`hdfs dfsadmin -report` annonce moins de 3 DataNodes** : un travailleur n'a pas démarré.
  `docker compose ps` montre lequel, `docker compose logs worker2` donne la raison. Le plus
  souvent, Docker manque de mémoire : augmentez-la dans les réglages de Docker Desktop.
- **L'historique MapReduce (19888) ne répond pas juste après le démarrage** : il démarre une fois
  que HDFS est sorti du safe mode, environ 20 secondes après le master. Attendez, puis rechargez.
- **La capacité HDFS annoncée (quelques To) dépasse votre disque** : les trois DataNodes écrivent
  sur le même disque, celui de Docker, et chacun en annonce la taille entière. C'est attendu.
- **Les fichiers déposés dans HDFS ont disparu** : `docker compose down -v` supprime les volumes.
  Utilisez `stop` et `start` entre deux séances, et redéposez les données sinon.
- **La première cellule Spark d'un notebook ne répond pas** : le serveur Spark Connect n'est pas
  joignable, et le client réessaie pendant plusieurs minutes avant d'abandonner. Vérifiez que le
  conteneur `client` tourne (`docker compose ps`) et que le serveur a démarré :
  `docker compose logs client | grep "Spark Connect server started"`.
- **`[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute rdd is not supported in Spark Connect`** (ou
  `sparkContext`) : l'API RDD ne passe pas par Spark Connect. Exprimez le calcul avec les
  DataFrames, par exemple `F.spark_partition_id()` pour connaître les partitions.
- **`Python in worker has different version: 3.10 than that in driver: 3.15`** : une fonction
  Python (UDF) s'exécute sur les workers, en Python 3.10, alors que votre notebook utilise une
  autre version. Les fonctions intégrées de Spark (`pyspark.sql.functions`) n'ont pas ce problème.
- **Un `spark-submit` reste bloqué sur `Initial job has not accepted any resources`** : une autre
  application Spark garde les 6 cœurs du cluster, le plus souvent le serveur Spark Connect juste
  après une requête d'un notebook. Il les rend au bout d'une à deux minutes sans requête ; on peut
  aussi le voir sur http://localhost:8080.
- **Un worker Spark annonce tous les cœurs et toute la mémoire de la machine** : rencontré pendant
  la préparation, avant de fixer `SPARK_WORKER_CORES` et `SPARK_WORKER_MEMORY` dans
  `docker-compose.yml`. Sans ces deux variables, chaque worker croit disposer de la machine entière.
