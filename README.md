
# Spark RDD - Total des Ventes par Ville

Ce projet est un TP de traitement de données avec **Apache Spark** et **Hadoop HDFS**, utilisant **RDD** en Java. L'objectif est de calculer le **total des ventes par ville** à partir d'un fichier texte contenant les ventes d'une entreprise.

---

## 1. Pré-requis

Avant de commencer, assurez-vous d'avoir installé :

- Docker et Docker Compose
- Java 17 ou supérieur
- Maven
- Spark et Hadoop (via Docker dans ce TP)

---

## 2. Structure du projet

```

RDD/
├── docker-compose.yml      # Conteneurs Spark + Hadoop
├── config                  # Configuration Hadoop (core-site.xml, etc.)
├── volumes/                # Volumes pour HDFS
├── RDDApp/                 # Projet Maven
│   ├── src/
│   │   └── main/
│   │       ├── java/com/rdd/spark/TotalVentesParVille.java
│   │       └── resources/ventes.txt
│   └── pom.xml
└── README.md

````

---

## 3. Démarrage du cluster Spark + HDFS

Le projet utilise **Docker Compose** pour déployer :

- HDFS : `namenode`, `datanode`
- YARN : `resourcemanager`, `nodemanager`
- Spark : `spark-master`, `spark-worker-1`

Pour démarrer le cluster :

```bash
docker-compose up -d
````

* HDFS NameNode UI : [http://localhost:9870](http://localhost:9870)
* YARN ResourceManager UI : [http://localhost:8088](http://localhost:8088)
* Spark Master UI : [http://localhost:8080](http://localhost:8080)

Vérifiez que tous les conteneurs sont **up** :

```bash
docker ps
```

---

## 4. Préparation des données

Le fichier `ventes.txt` contient les ventes par date, ville, produit et prix :

```
2025-01-01 Casablanca Laptop 1000
2025-01-02 Rabat Smartphone 500
2025-01-01 Casablanca Smartphone 300
2025-01-03 Marrakech Laptop 1200
2024-12-31 Rabat Laptop 800
```

### 4.1 Copier le fichier dans HDFS

1. Entrer dans le conteneur NameNode :

```bash
docker exec -it rdd-namenode bash
```

2. Créer le répertoire d’input dans HDFS :

```bash
hdfs dfs -mkdir -p /input
```

3. Copier le fichier :

```bash
hdfs dfs -put /path/to/ventes.txt /input/
```

4. Vérifier :

```bash
hdfs dfs -ls /input
```

---

## 5. Développement et test local

La classe principale `TotalVentesParVille.java` se trouve dans `src/main/java/com/rdd/spark/`.

Pour tester l’application **en local** (avant d’utiliser le cluster) :

```bash
mvn clean package
spark-submit \
  --class com.rdd.spark.TotalVentesParVille \
  --master local[*] \
  target/RDDApp-1.0-SNAPSHOT.jar
```

> Note : Ici, `textFile` lit le fichier depuis `src/main/resources/ventes.txt`.

---

## 6. Exécution sur le cluster Spark + HDFS

### 6.1 Préparer le fichier JAR

1. Packager le projet Maven :

```bash
mvn clean package
```

2. Copier le JAR dans le conteneur Spark Master :

```bash
docker cp target/RDDApp-1.0-SNAPSHOT.jar spark-master:/opt/spark/work-dir/
```

> Si permission denied, utilisez `sudo` ou changez le propriétaire du répertoire dans le conteneur.

### 6.2 Modifier le chemin du fichier dans le code

Dans `TotalVentesParVille.java` :

```java
JavaRDD<String> lignes = sc.textFile("hdfs://namenode:8020/input/ventes.txt");
```

### 6.3 Exécuter le job Spark sur le cluster

Entrer dans le conteneur Spark Master :

```bash
docker exec -it spark-master bash
```

Exécuter le job :

```bash
/opt/spark/bin/spark-submit \
  --class com.rdd.spark.TotalVentesParVille \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/RDDApp-1.0-SNAPSHOT.jar
```

> Le programme lit les données depuis HDFS et affiche le **total des ventes par ville**.

---

## 7. Résultats attendus

Pour le fichier exemple :

```
(Casablanca,1300.0)
(Rabat,1300.0)
(Marrakech,1200.0)
```

---

## 8. Étapes clés

1. Démarrer le cluster Spark + Hadoop via Docker Compose
2. Copier le fichier `ventes.txt` dans HDFS
3. Développer et tester l’application localement avec Maven et `spark-submit`
4. Packager le projet en JAR
5. Copier le JAR dans Spark Master
6. Modifier le chemin du fichier pour pointer vers HDFS
7. Soumettre le job Spark sur le cluster

---

## 9. Notes

* Pour voir le Spark UI pendant l’exécution : [http://localhost:8080](http://localhost:8080)
* Pour voir le HDFS NameNode UI : [http://localhost:9870](http://localhost:9870)
* Pour le YARN ResourceManager UI : [http://localhost:8088](http://localhost:8088)
* Vous pouvez étendre ce projet pour calculer **les ventes par ville et par année** (Exercice 2) en utilisant `map` et `reduceByKey` sur `(ville, année)`.


## 10. Exercice 2 – Analyse de logs avec RDD

Cet exercice consiste à analyser un fichier de logs Apache pour extraire des informations utiles comme le nombre de requêtes, le nombre d’erreurs, le top des IP et des ressources les plus demandées.

### 10.1 Préparation du fichier de logs

Le fichier `access.log` contient des lignes au format Apache :

```
127.0.0.1 - - [10/Oct/2025:09:15:32 +0000] "GET /index.html HTTP/1.1" 200 1024 "-" "Mozilla/5.0"
192.168.1.10 - john [10/Oct/2025:09:17:12 +0000] "POST /login HTTP/1.1" 302 512 "-" "curl/7.68.0"
203.0.113.5 - - [10/Oct/2025:09:19:01 +0000] "GET /docs/report.pdf HTTP/1.1" 404 64 "-" "Mozilla/5.0"
```

### 10.2 Copier le fichier dans HDFS

1. Entrer dans le conteneur NameNode :

```bash
docker exec -it rdd-namenode bash
```

2. Créer le répertoire `input` si ce n’est pas déjà fait :

```bash
hdfs dfs -mkdir -p /input
```

3. Copier le fichier `access.log` :

```bash
hdfs dfs -put /path/to/access.log /input/
```

4. Vérifier :

```bash
hdfs dfs -ls /input
```

### 10.3 Développement de l’application

Le code Java utilise Spark **RDD** pour :

1. Charger le fichier depuis HDFS :

```java
JavaRDD<String> logs = sc.textFile("hdfs://namenode:8020/input/access.log");
```

2. Extraire les champs : IP, date/heure, méthode HTTP, ressource, code HTTP, taille.

3. Calculer :

* Nombre total de requêtes
* Nombre et pourcentage d’erreurs (code ≥ 400)
* Top 5 des IP les plus actives
* Top 5 des ressources les plus demandées
* Répartition des requêtes par code HTTP

4. Afficher les résultats dans la console.

> La classe principale est `LogAnalysis.java` et l’entité `LogEntry.java` sert à stocker les informations extraites.

### 10.4 Packager et exécuter l’application

1. Compiler et packager le projet Maven :

```bash
mvn clean package
```

2. Copier le JAR dans Spark Master :

```bash
docker cp target/RDDApp-1.0-SNAPSHOT.jar spark-master:/opt/spark/work-dir/
```

3. Entrer dans le conteneur Spark Master :

```bash
docker exec -it spark-master bash
```

4. Soumettre le job Spark :

```bash
/opt/spark/bin/spark-submit \
  --class com.rdd.spark.LogAnalysis \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/RDDApp-1.0-SNAPSHOT.jar
```

> L’application lit le fichier depuis HDFS et affiche les statistiques et les tops directement dans la console.

### 10.5 Résultats attendus

Exemple de sortie :

```
Total requests: 6
Total errors: 2
Error percentage: 33.33%
Top 5 IPs:
192.168.1.10 -> 2 requests
127.0.0.1 -> 2 requests
203.0.113.5 -> 1 requests
198.51.100.7 -> 1 requests
Top 5 resources:
/index.html -> 2 hits
/login -> 1 hits
/docs/report.pdf -> 1 hits
/api/data?id=123 -> 1 hits
/dashboard -> 1 hits
HTTP code distribution:
200 -> 3
302 -> 1
404 -> 1
500 -> 1
```

---

### 10.6 Étapes clés

1. Préparer le fichier `access.log` et le copier dans HDFS
2. Développer `LogAnalysis.java` et `LogEntry.java`
3. Compiler et packager le projet en JAR
4. Copier le JAR dans Spark Master
5. Exécuter le job sur le cluster Spark en utilisant `spark-submit`

> Cette approche montre comment traiter des fichiers volumineux avec Spark et HDFS en utilisant les **RDD Java**, et comment obtenir des statistiques exploitables à partir de logs web.




