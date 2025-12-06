
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



