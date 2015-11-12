package it.unitn.spark.examples;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

import org.apache.spark.SparkConf;


public class JavaWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("wordCountInput.txt");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		System.out.println("TOTAL: " + totalLength);
		
		JavaRDD<String> flat = lines
				.flatMap(x -> Arrays.asList(x.replaceAll("[^A-Za-z ]", "").split(" ")));
		JavaPairRDD<String, Integer> map = flat
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		
		JavaPairRDD<String, Integer> reduce = map
				.reduceByKey((x, y) -> x + y);
		
		System.out.println(reduce.collect());
			
		sc.stop();
		sc.close();
	}
}
