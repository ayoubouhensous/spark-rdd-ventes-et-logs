package com.rdd.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogAnalysis {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("LogAnalysis")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier (local ou HDFS)
         //JavaRDD<String> logs = sc.textFile("src/main/resources/access.log");
        JavaRDD<String> logs = sc.textFile("hdfs://namenode:8020/input/access.log");

        // Regex pour extraire les champs
        Pattern logPattern = Pattern.compile(
                "(\\d+\\.\\d+\\.\\d+\\.\\d+) - (\\S+) \\[(.+?)\\] \"(\\S+) (\\S+) .*\" (\\d+) (\\d+|-)"
        );

        // Extraction des champs
        JavaRDD<LogEntry> logEntries = logs.map(line -> {
            Matcher m = logPattern.matcher(line);
            if (m.find()) {
                String ip = m.group(1);
                String date = m.group(3);
                String method = m.group(4);
                String resource = m.group(5);
                int code = Integer.parseInt(m.group(6));
                int size = m.group(7).equals("-") ? 0 : Integer.parseInt(m.group(7));
                return new LogEntry(ip, date, method, resource, code, size);
            } else {
                return null;
            }
        }).filter(entry -> entry != null);

        // Nombre total de requêtes
        long totalRequests = logEntries.count();

        // Nombre total d'erreurs (code >= 400)
        long totalErrors = logEntries.filter(e -> e.getCode() >= 400).count();

        double errorPercentage = (totalErrors * 100.0) / totalRequests;

        System.out.println("Total requests: " + totalRequests);
        System.out.println("Total errors: " + totalErrors);
        System.out.println("Error percentage: " + errorPercentage + "%");

        // Top 5 IPs les plus actives
        JavaPairRDD<String, Integer> ipCounts = logEntries
                .mapToPair(e -> new Tuple2<>(e.getIp(), 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<Integer, String>> topIPs = ipCounts
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .take(5);

        System.out.println("Top 5 IPs:");
        topIPs.forEach(t -> System.out.println(t._2 + " -> " + t._1 + " requests"));

        // Top 5 ressources demandées
        JavaPairRDD<String, Integer> resourceCounts = logEntries
                .mapToPair(e -> new Tuple2<>(e.getResource(), 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<Integer, String>> topResources = resourceCounts
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .take(5);

        System.out.println("Top 5 resources:");
        topResources.forEach(t -> System.out.println(t._2 + " -> " + t._1 + " hits"));

        // Répartition par code HTTP
        JavaPairRDD<Integer, Integer> codeCounts = logEntries
                .mapToPair(e -> new Tuple2<>(e.getCode(), 1))
                .reduceByKey(Integer::sum);

        System.out.println("HTTP code distribution:");
        codeCounts.collect().forEach(t -> System.out.println(t._1 + " -> " + t._2));

        sc.close();
    }
}
