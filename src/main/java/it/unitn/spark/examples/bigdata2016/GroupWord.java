package it.unitn.spark.examples.bigdata2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class GroupWord {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("files/inferno.txt");

		sc.stop();
		sc.close();
	}
}
