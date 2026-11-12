"""Démo : blocs, copies et panne d'un DataNode.

Les logs HTTP du serveur web du Kennedy Space Center (NASA), juillet et août 1995, sont déposés dans
HDFS avec des blocs de 32 Mo. On regarde où sont les blocs et leurs copies, puis on arrete un
DataNode et on observe ce que fait le NameNode. Enfin on compare un gros fichier et ses fichiers journaliers
journaliers du point de vue du NameNode.

Critères de réussite (vérifiés par la commande `comparer`) :
  1. Le nombre de blocs vaut ceil(taille / 32 Mo) et chaque bloc à 3 copies vivantes.
  2. Pendant la panne, le fichier relu a le même md5 que l'original.
  3. Après la panne, le fichier à 2 copies retrouve 2 copies vivantes sur les nœuds restants, et le
     fichier à 3 copies reste sous-répliqué (7 blocs) : il n'y a plus que 2 nœuds.

Usage (depuis ce dossier, cluster démarré) :
    uv run python run.py telecharger   # 37 Mo compressés, à faire avant le cours
    uv run python run.py tout          # blocs, copies, panne, petits_fichiers, comparer
"""

import gzip
import hashlib
import json
import math
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ICI = Path(__file__).resolve().parent
RACINE = ICI.parents[1]                       # dossier du module, ou se trouve docker-compose.yml
DATA = ICI / "data"
RESULTATS = ICI / "resultats"
DANS_LE_CLIENT = "/cours/" + ICI.relative_to(RACINE).as_posix()

SOURCE = "https://ita.ee.lbl.gov/traces"
MOIS = ["Jul95", "Aug95"]
BLOC = 32 * 1024 * 1024                       # taille de bloc imposée a la demo : 32 Mo
RACINE_HDFS = "/demo/nasa"
GROS = f"{RACINE_HDFS}/gros"                  # juillet en un seul fichier, 3 copies
DEUX = f"{RACINE_HDFS}/deux_copies"           # août en un seul fichier, 2 copies
JOURS = f"{RACINE_HDFS}/jours"                # juillet en fichiers journaliers
FICHIER_JUL = "NASA_access_log_Jul95"
FICHIER_AOUT = "NASA_access_log_Aug95"
DELAI_PANNE = 180                             # secondes d'attente maximum pour chaque étape de panne


def commande(argv, capture=True, echec_fatal=True):
    resultat = subprocess.run(argv, capture_output=capture, text=True)
    if resultat.returncode != 0 and echec_fatal:
        sys.exit(f"Échec de : {' '.join(argv)}\n{resultat.stderr or ''}")
    return resultat


def client(cmd, echec_fatal=True):
    """Exécute une commande bash dans le conteneur client du cluster."""
    return commande(["docker", "compose", "-f", str(RACINE / "docker-compose.yml"), "exec", "-T",
                     "client", "bash", "-c", cmd], echec_fatal=echec_fatal).stdout


def compose(*args):
    return commande(["docker", "compose", "-f", str(RACINE / "docker-compose.yml"), *args])


def ecrire(nom, valeurs):
    RESULTATS.mkdir(exist_ok=True)
    (RESULTATS / f"{nom}.json").write_text(json.dumps(valeurs, indent=2, ensure_ascii=False) + "\n")


def lire(nom):
    chemin = RESULTATS / f"{nom}.json"
    if not chemin.exists():
        sys.exit(f"{chemin.name} manque : lancez d'abord l'étape correspondante")
    return json.loads(chemin.read_text())


def telecharger():
    DATA.mkdir(exist_ok=True)
    for mois in MOIS:
        archive = DATA / f"NASA_access_log_{mois}.gz"
        texte = DATA / f"NASA_access_log_{mois}"
        if not archive.exists():
            print(f"Téléchargement de {archive.name}")
            urllib.request.urlretrieve(f"{SOURCE}/{archive.name}", archive)
        with gzip.open(archive, "rb") as entree, open(texte, "wb") as sortie:
            shutil.copyfileobj(entree, sortie)
        print(f"{texte.name} : {texte.stat().st_size:,} octets".replace(",", " "))
    decouper_par_jour()


def decouper_par_jour():
    """Découpe le fichier de juillet en un fichier par jour, d'après l'horodatage de chaque ligne."""
    dossier = DATA / "jours"
    shutil.rmtree(dossier, ignore_errors=True)
    dossier.mkdir()
    ouverts = {}
    motif = re.compile(rb"\[(\d{2})/Jul/1995:")
    jour = b"00"                              # les lignes sans horodatage restent avec le jour précédent
    with open(DATA / FICHIER_JUL, "rb") as entree:
        for ligne in entree:
            trouve = motif.search(ligne)
            if trouve:
                jour = trouve.group(1)
            if jour not in ouverts:
                ouverts[jour] = open(dossier / f"jul95_{jour.decode()}.log", "wb")
            ouverts[jour].write(ligne)
    for f in ouverts.values():
        f.close()
    print(f"{len(ouverts)} fichiers journaliers dans {dossier.name}/")


def verifier_donnees():
    """Télécharge les logs et les fichiers journaliers s'ils manquent : l'étape part toujours de données présentes."""
    if not all((DATA / f"NASA_access_log_{m}").exists() for m in MOIS) or not (DATA / "jours").exists():
        print("Données absentes : téléchargement (37 Mo)")
        telecharger()


def verifier_montage():
    """Le dossier du module est monté dans le conteneur client sous /cours : celui de ce dépôt, pas un autre."""
    resultat = subprocess.run(
        ["docker", "compose", "-f", str(RACINE / "docker-compose.yml"), "exec", "-T", "client", "test", "-f",
         f"{DANS_LE_CLIENT}/data/{FICHIER_JUL}"], capture_output=True, text=True)
    if resultat.returncode != 0:
        monte = subprocess.run(
            ["docker", "inspect", "-f", '{{range .Mounts}}{{if eq .Destination "/cours"}}{{.Source}}{{end}}{{end}}',
             "bigdata-client-1"], capture_output=True, text=True).stdout.strip()
        sys.exit(f"Le conteneur client ne voit pas {DANS_LE_CLIENT}/data/{FICHIER_JUL}.\n"
                 f"Dossier monté sous /cours : {monte or 'inconnu'} ; dossier de ce dépôt : {RACINE}.\n"
                 "Le cluster a été démarré depuis un autre dossier : arrêtez-le (docker compose down, depuis le "
                 "dossier où il a été lancé), puis relancez-le depuis ce dépôt (docker compose up -d --build).")


def reinitialiser():
    """État de départ : /demo/nasa vide, les trois DataNodes démarrés."""
    verifier_donnees()
    verifier_montage()
    compose("start", "worker1", "worker2", "worker3")
    attendre(lambda: noeuds()["vivants"] == 3, "trois DataNodes vivants")
    adresses()
    client(f"hdfs dfs -rm -r -f -skipTrash {RACINE_HDFS}; hdfs dfs -mkdir -p {RACINE_HDFS}")


def attendre(condition, description, delai=DELAI_PANNE):
    debut = time.time()
    while not condition():
        if time.time() - debut > delai:
            sys.exit(f"Délai dépassé ({delai} s) : {description}")
        time.sleep(3)
    return round(time.time() - debut)


def noeuds():
    rapport = client("hdfs dfsadmin -report")
    vivants = re.search(r"Live datanodes \((\d+)\)", rapport)
    morts = re.search(r"Dead datanodes \((\d+)\)", rapport)
    return {"vivants": int(vivants.group(1)) if vivants else 0,
            "morts": int(morts.group(1)) if morts else 0}


NOMS = {}                                     # adresse IP vers nom du travailleur, mémorisée tant que les 3 vivent


def adresses():
    """Adresse IP vers nom du travailleur (worker1, ...), lue dans le rapport du NameNode.

    Le rapport n'affiche plus le nom d'un DataNode mort : la table est donc lue quand les trois
    DataNodes sont vivants (au depart de chaque étape) et réutilisée ensuite.
    """
    rapport = client("hdfs dfsadmin -report")
    for ip, nom in re.findall(r"^Name: ([\d.]+):9866 \((\S+)\)", rapport, re.M):
        trouve = re.search(r"bigdata-(worker\d)", nom)
        if trouve:
            NOMS[ip] = trouve.group(1)
    return NOMS


def blocs_de(chemin):
    """Liste des blocs d'un fichier : (numero, longueur, copies vivantes, travailleurs)."""
    noms = adresses()
    sortie = client(f"hdfs fsck {chemin} -files -blocks -locations 2>/dev/null")
    blocs = []
    for ligne in sortie.splitlines():
        m = re.match(r"(\d+)\. \S+ len=(\d+) Live_repl=(\d+)\s+\[(.*)\]", ligne)
        if m:
            ips = re.findall(r"DatanodeInfoWithStorage\[([\d.]+):9866", m.group(4))
            blocs.append({"bloc": int(m.group(1)), "octets": int(m.group(2)),
                          "copies": int(m.group(3)), "sur": sorted(noms.get(ip, ip) for ip in ips)})
    return blocs


def sous_repliques(chemin):
    sortie = client(f"hdfs fsck {chemin} 2>/dev/null")
    return int(re.search(r"Under-replicated blocks:\s+(\d+)", sortie).group(1))


def md5_hdfs(chemin):
    return client(f"hdfs dfs -cat {chemin} | md5sum").split()[0]


def md5_local(chemin):
    h = hashlib.md5()
    with open(chemin, "rb") as f:
        while bloc := f.read(1 << 20):
            h.update(bloc)
    return h.hexdigest()


def attendre_copies(chemin):
    """Attend que tous les blocs d'un fichier aient leurs copies : le dépôt rend la main avant la dernière."""
    attendre(lambda: sous_repliques(chemin) == 0, f"copies complètes de {chemin}")


def afficher_blocs(blocs):
    for b in blocs:
        print(f"  bloc {b['bloc']}  {b['octets']:>10,} o  {b['copies']} copies  {', '.join(b['sur'])}"
              .replace(",", " "))


def blocs():
    """Dépose juillet en blocs de 32 Mo, réglage de la demo, et montre où sont les blocs."""
    reinitialiser()
    local = DATA / FICHIER_JUL
    client(f"hdfs dfs -D dfs.blocksize={BLOC} -put {DANS_LE_CLIENT}/data/{FICHIER_JUL} {GROS}")
    attendre_copies(GROS)
    liste = blocs_de(GROS)
    afficher_blocs(liste)
    taille = local.stat().st_size
    ecrire("blocs", {"octets": taille, "blocs_attendus": math.ceil(taille / BLOC),
                     "blocs": liste, "stat": client(f'hdfs dfs -stat "%o %r %b" {GROS}').strip(),
                     "du": client(f"hdfs dfs -du {GROS}").split()[:2]})


def copies():
    """Change le facteur de réplication d'un fichier et observe l'espace consommé."""
    lu = lire("blocs")
    mesures = []
    for facteur in (2, 3):
        client(f"hdfs dfs -setrep -w {facteur} {GROS}")
        du = client(f"hdfs dfs -du {GROS}").split()
        mesures.append({"facteur": facteur, "taille": int(du[0]), "espace_disque": int(du[1])})
        print(f"  réplication {facteur} : taille {du[0]} o, espace consommé {du[1]} o")
    ecrire("copies", {"mesures": mesures, "taille": lu["octets"]})


def panne():
    """Arrête worker1 : le NameNode le déclaré mort, puis re-réplique ce qu'il peut."""
    reinitialiser()
    client(f"hdfs dfs -D dfs.blocksize={BLOC} -put {DANS_LE_CLIENT}/data/{FICHIER_JUL} {GROS}")
    client(f"hdfs dfs -D dfs.blocksize={BLOC} -D dfs.replication=2 "
           f"-put {DANS_LE_CLIENT}/data/{FICHIER_AOUT} {DEUX}")
    attendre_copies(GROS)
    attendre_copies(DEUX)
    avant = {"gros": blocs_de(GROS), "deux": blocs_de(DEUX)}
    print("Avant la panne, fichier à 2 copies :")
    afficher_blocs(avant["deux"])
    blocs_sur_w1 = sum("worker1" in b["sur"] for b in avant["deux"])
    try:
        compose("stop", "worker1")
        debut = time.time()
        mort = attendre(lambda: noeuds()["morts"] == 1, "worker1 déclaré mort")
        print(f"worker1 déclaré mort après {mort} s")
        md5_pendant = md5_hdfs(GROS)
        sous_gros = sous_repliques(GROS)
        # Le fichier à 2 copies doit retrouver 2 copies vivantes sur les deux nœuds restants
        attendre(lambda: sous_repliques(DEUX) == 0, "re-réplication du fichier à 2 copies")
        reparation = round(time.time() - debut)
        print(f"fichier à 2 copies de nouveau complet après {reparation} s")
        apres = {"gros": blocs_de(GROS), "deux": blocs_de(DEUX)}
        print("Après la panne, fichier à 2 copies :")
        afficher_blocs(apres["deux"])
        sous_gros_final = sous_repliques(GROS)
    finally:
        compose("start", "worker1")
    attendre(lambda: noeuds()["vivants"] == 3, "worker1 de retour")
    retour = attendre(lambda: sous_repliques(GROS) == 0, "fichier à 3 copies de nouveau complet")
    print(f"worker1 de retour : fichier à 3 copies complet {retour} s après")
    ecrire("panne", {"mort_apres_s": mort, "reparation_deux_copies_s": reparation,
                     "blocs_du_fichier_a_2_copies_sur_worker1": blocs_sur_w1,
                     "md5_pendant_la_panne": md5_pendant,
                     "sous_repliques_gros_pendant": sous_gros,
                     "sous_repliques_gros_apres_reparation": sous_gros_final,
                     "sous_repliques_gros_apres_retour": sous_repliques(GROS),
                     "avant": avant, "apres": apres})


def nombre_objets(chemin):
    """Fichiers et blocs d'un chemin, d'après le NameNode (hdfs dfs -count et fsck)."""
    fichiers = int(client(f"hdfs dfs -count {chemin}").split()[1])
    blocs_total = int(re.search(r"Total blocks \(validated\):\s+(\d+)",
                                client(f"hdfs fsck {chemin} 2>/dev/null")).group(1))
    return fichiers, blocs_total


def petits_fichiers():
    """Compare juillet en un fichier et en fichiers journaliers, taille de bloc par défaut (128 Mo)."""
    reinitialiser()
    mesures = {}
    for nom, source, cible in (("un_fichier", f"data/{FICHIER_JUL}", GROS),
                               ("jours", "data/jours/*", JOURS)):
        if nom == "jours":
            client(f"hdfs dfs -mkdir -p {JOURS}")
        debut = time.perf_counter()
        client(f"hdfs dfs -put {DANS_LE_CLIENT}/{source} {cible if nom == 'un_fichier' else JOURS + '/'}")
        duree = time.perf_counter() - debut
        fichiers, nb_blocs = nombre_objets(cible)
        espace = int(client(f"hdfs dfs -du -s {cible}").split()[1])
        mesures[nom] = {"fichiers": fichiers, "blocs": nb_blocs, "objets": fichiers + nb_blocs,
                        "espace_disque": espace, "depot_s": round(duree, 1)}
        print(f"  {nom:<10} {fichiers:>3} fichiers  {nb_blocs:>3} blocs  "
              f"espace {espace:,} o  depot {duree:.1f} s".replace(",", " "))
    ecrire("petits_fichiers", mesures)


def comparer():
    b, p, pf = lire("blocs"), lire("panne"), lire("petits_fichiers")
    ok = True

    def critere(texte, vrai):
        nonlocal ok
        ok &= vrai
        print(f"  [{'ok' if vrai else 'ECHEC'}] {texte}")

    print("Critère 1 : blocs et copies")
    critere(f"{len(b['blocs'])} blocs, attendu ceil({b['octets']} / {BLOC}) = {b['blocs_attendus']}",
            len(b["blocs"]) == b["blocs_attendus"])
    critere("chaque bloc à 3 copies vivantes", all(x["copies"] == 3 for x in b["blocs"]))
    print("Critère 2 : le contenu survit à la panne")
    critere(f"md5 relu pendant la panne {p['md5_pendant_la_panne']}",
            p["md5_pendant_la_panne"] == md5_local(DATA / FICHIER_JUL))
    print("Critère 3 : ce que HDFS peut et ne peut pas réparer")
    critere(f"{p['blocs_du_fichier_a_2_copies_sur_worker1']} blocs du fichier à 2 copies étaient sur worker1, "
            "tous de nouveau à 2 copies",
            all(x["copies"] == 2 for x in p["apres"]["deux"]))
    critere(f"fichier à 3 copies : {p['sous_repliques_gros_apres_reparation']} blocs sous-répliqués "
            f"(2 nœuds restants), puis {p['sous_repliques_gros_apres_retour']} au retour de worker1",
            p["sous_repliques_gros_apres_reparation"] == len(b["blocs"])
            and p["sous_repliques_gros_apres_retour"] == 0)
    print("Petits fichiers (blocs de 128 Mo)")
    for nom, m in pf.items():
        print(f"  {nom:<10} {m['fichiers']:>3} fichiers + {m['blocs']:>3} blocs = {m['objets']:>3} objets, "
              f"espace {m['espace_disque']:,} o".replace(",", " "))
    sys.exit(0 if ok else "Un critère n'est pas satisfait")


ETAPES = {"telecharger": telecharger, "blocs": blocs, "copies": copies, "panne": panne,
          "petits_fichiers": petits_fichiers, "comparer": comparer}


if __name__ == "__main__":
    demande = sys.argv[1] if len(sys.argv) > 1 else "tout"
    if demande == "tout":
        for etape in ("blocs", "copies", "panne", "petits_fichiers", "comparer"):
            print(f"\n== {etape}")
            ETAPES[etape]()
    elif demande in ETAPES:
        ETAPES[demande]()
    else:
        sys.exit(f"Étape inconnue : {demande}. Choix : tout, {', '.join(ETAPES)}")
