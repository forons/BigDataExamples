package it.unitn.spark.examples.bigdata2016;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Palindromes {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Palindromes").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("files/inferno.txt");
		JavaRDD<String> filtered = lines.flatMap(line -> Arrays.asList(line.toLowerCase().replaceAll("[^A-Za-z ]", "").split(" ")).iterator());
		JavaPairRDD<String, Boolean> pair = filtered
				.mapToPair(w -> new Tuple2<String, Boolean>(w, w.equals(new StringBuilder(w).reverse().toString()))).reduceByKey((x, y) -> x);

		JavaPairRDD<String, Boolean> filter = pair.filter(x -> x._2);

		List<Tuple2<String, Boolean>> list = filter.collect();

		for (Tuple2<String, Boolean> tup : list)
			System.out.println(tup._1 + ":" + tup._2);
//		System.out.println(filter.collect());

		sc.stop();
		sc.close();
	}
}
