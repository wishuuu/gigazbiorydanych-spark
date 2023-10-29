package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("st46688-lab2")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> movies = sc.textFile("C:\\Users\\wo46688\\IdeaProjects\\untitled\\src\\main\\java\\org\\example\\movies.csv");
        JavaRDD<String> ratings = sc.textFile("C:\\Users\\wo46688\\IdeaProjects\\untitled\\src\\main\\java\\org\\example\\ratings.csv");

        movies = movies.filter(x -> !x.isEmpty() && !x.startsWith("movieId"));
        ratings = ratings.filter(x -> !x.isEmpty() && !x.startsWith("userId"));

        JavaRDD<String> ratings_movieIds = ratings.map(rating -> Arrays.asList(rating.split(",")).get(1));

        JavaPairRDD<String, Integer> ratings_pairs = ratings_movieIds.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = ratings_pairs.reduceByKey((a, b) -> a + b);
        counts.foreach(el -> System.out.println(el));

//        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("[ ,;\\.]")).iterator());
//        JavaPairRDD<String, Integer> paris = words.mapToPair(word -> new Tuple2<>(word, 1));
//        JavaPairRDD<String, Integer> counts = paris.reduceByKey((a, b) -> a + b);
//        counts.foreach(el -> System.out.println(el));
    }
}