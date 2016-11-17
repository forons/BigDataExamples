package it.unitn.spark.examples.bigdata2015;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class JavaAnagramms {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaAnagramms").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("test.txt");
		JavaRDD<Integer> lineLengths = input.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		System.out.println("TOTAL: " + totalLength);

		JavaRDD<String> flat = input.flatMap(x -> Arrays.asList(x.replaceAll("[^A-Za-z ]", "").split(" ")).iterator());
		JavaPairRDD<String, String> map = flat.mapToPair(x -> new Tuple2<String, String>(orderLexicographically(x), x));

		JavaPairRDD<String, String> reduce = map.reduceByKey((x, y) -> x + "~" + y);

		System.out.println(reduce.collect());

		sc.stop();
		sc.close();
	}

	public static String orderLexicographically(String input) {
		char[] cArr = input.toCharArray();
		Arrays.sort(cArr);
		return new String(cArr);
	}
}