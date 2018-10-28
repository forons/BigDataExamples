package it.unitn.spark.examples.bigdata2016;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings({"serial", "unused"})
public class WordsOccurrences {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("files/inferno.txt");
		// JavaPairRDD<String, Integer> pairWithLambda =
		// wordCountWithLambda(lines);
		// System.out.println(pairWithLambda.collect());

		JavaPairRDD<String, Integer> pairWithoutLambda = wordCountWithoutLambda(lines);
		System.out.println(pairWithoutLambda.collect());
		sc.stop();
		sc.close();
	}

	private static JavaPairRDD<String, Integer> wordCountWithLambda(JavaRDD<String> lines) {
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.toLowerCase()
				.replaceAll("[^A-Za-z ]", "")
				.split(" "))
				.iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1))
				.reduceByKey((x, y) -> x + y);
		return pairs.filter(x -> x._2 == 5 ? true : false);
	}

	private static JavaPairRDD<String, Integer> wordCountWithoutLambda(JavaRDD<String> lines) {
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) {
				return Arrays.asList(line.toLowerCase()
						.replaceAll("[^a-z ]", "")
						.split(" "))
						.iterator();
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});
		JavaPairRDD<String, Integer> filtered = counts
				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Integer> tup) throws Exception {
						if (tup._2 == 5)
							return true;
						return false;
					}
				});

		return filtered;
	}
}
