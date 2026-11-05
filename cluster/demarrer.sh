#!/usr/bin/env bash
# Démarre les services d'un nœud selon son rôle : master, worker ou client.
set -euo pipefail

mkdir -p "$HADOOP_LOG_DIR"

case "${1:-}" in
  master)
    # Au premier démarrage seulement : formater l'espace de noms HDFS
    if [ ! -d /data/nn/current ]; then
      hdfs namenode -format -nonInteractive -clusterId cours
    fi
    hdfs --daemon start namenode
    yarn --daemon start resourcemanager
    "$SPARK_HOME"/sbin/start-master.sh
    # Le serveur d'historique MapReduce écrit dans HDFS : il attend la sortie du safe mode
    (hdfs dfsadmin -safemode wait \
      && hdfs dfs -mkdir -p /tmp /user/spark \
      && hdfs dfs -chmod 1777 /tmp \
      && mapred --daemon start historyserver) &
    ;;
  worker)
    hdfs --daemon start datanode
    yarn --daemon start nodemanager
    "$SPARK_HOME"/sbin/start-worker.sh spark://master:7077
    ;;
  client)
    # Serveur Spark Connect (port 15002) : les notebooks des étudiants s'y connectent depuis leur
    # machine. Son driver tourne ici. L'allocation dynamique rend au master les cœurs inactifs,
    # pour qu'un spark-submit lancé depuis ce conteneur les obtienne ; le parallélisme par défaut
    # reste celui des 6 cœurs du cluster, même quand aucun executor n'est encore lancé.
    "$SPARK_HOME"/sbin/start-connect-server.sh \
      --name spark-connect \
      --conf spark.default.parallelism=6 \
      --conf spark.dynamicAllocation.enabled=true \
      --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
      --conf spark.dynamicAllocation.shuffleTracking.timeout=120s \
      --conf spark.dynamicAllocation.executorIdleTimeout=60s
    ;;
  *)
    exec "$@"
    ;;
esac

sleep 3
exec tail -n +1 -F "$HADOOP_LOG_DIR"/*.log "$SPARK_HOME"/logs/*.out
