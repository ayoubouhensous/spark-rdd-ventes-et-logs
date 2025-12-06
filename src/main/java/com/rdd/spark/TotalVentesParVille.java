package com.rdd.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class TotalVentesParVille {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVille");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier
        // JavaRDD<String> lignes = sc.textFile("src/main/resources/ventes.txt");
        JavaRDD<String> lignes = sc.textFile("hdfs://namenode:8020/input/ventes.txt");

        // Transformer en paire (ville, prix)
        JavaPairRDD<String, Double> paires = lignes.mapToPair(ligne -> {
            String[] parts = ligne.split(" ");
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        });

        // Somme par ville
        JavaPairRDD<String, Double> totalParVille = paires.reduceByKey((a, b) -> a + b);

        // Afficher
        totalParVille.collect().forEach(System.out::println);

        sc.close();
    }
}
