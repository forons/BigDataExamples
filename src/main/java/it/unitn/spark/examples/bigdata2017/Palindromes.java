package it.unitn.spark.examples.bigdata2017;

import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Palindromes {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Palindrome");
    if (new File("/Users/").exists()) {
      conf.setMaster("local");
    }
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rdd = sc.textFile("files/data.txt");
    rdd = rdd.filter((Function<String, Boolean>) string -> string
        .equals(new StringBuilder(string).reverse().toString()));

    for (String string : rdd.collect()) {
      System.out.println(string);
    }
  }
}
