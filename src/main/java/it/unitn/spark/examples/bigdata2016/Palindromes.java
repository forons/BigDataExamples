package it.unitn.spark.examples.bigdata2016;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Palindromes {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Palindromes").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("files/test.txt");
		JavaRDD<String> filtered = lines.flatMap(line -> Arrays.asList(line.toLowerCase().replaceAll("[^A-Za-z ]", "")).iterator());
		JavaPairRDD<String, Boolean> pair = filtered
				.mapToPair(w -> new Tuple2<String, Boolean>(w, w.equals(new StringBuilder(w).reverse().toString()))).reduceByKey((x, y) -> x);

		System.out.println(pair.collect());

		sc.stop();
		sc.close();
	}
}
