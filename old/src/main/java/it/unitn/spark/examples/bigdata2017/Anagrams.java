package it.unitn.spark.examples.bigdata2017;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Anagrams {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Anagrams");
    if(new File("/Users/").exists()) {
      conf.setMaster("local");
    }
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rdd = sc.textFile("files/data.txt");
    JavaPairRDD<String, List<String>> anagrams = rdd.mapToPair((PairFunction<String, String, List<String>>) s -> {
      List<Character> chars = new ArrayList<>();
      for(char c : s.toCharArray()){
        chars.add(c);
      }
      Collections.sort(chars);
      List<String> words = new ArrayList<>();
      words.add(s);
      StringBuilder builder = new StringBuilder();
      for(char c : chars) {
        builder.append(c);
      }
      return new Tuple2<>(builder.toString(), words);
    }).reduceByKey((x, y) -> {
      x.addAll(y);
      return x;
    }).filter(x -> x._2.size() > 1);
    

    for (Entry<String, List<String>> entry : anagrams.collectAsMap().entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }
  }
}
