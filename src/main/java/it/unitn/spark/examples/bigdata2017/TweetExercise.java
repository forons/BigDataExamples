package it.unitn.spark.examples.bigdata2017;

import java.io.File;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class TweetExercise {

  public static void main(String[] args) {

    Builder builder = new Builder().appName("SparkSQL Examples");
    if (new File("/Users/").exists()) {
      builder.master("local");
    }
    SparkSession spark = builder.getOrCreate();

    Dataset<Row> tweets = spark.read().json("files/tweets.json");

    tweets.printSchema();
    tweets.show(false);
    
//    System.exit(1);

    Dataset<Row> countPerUser = tweets.groupBy("user.name").count().sort(col("count").desc())
        .limit(20);
    System.out.println("========================");
    System.out.println("========================");
    System.out.println("Count Per User");
    for (Row count : countPerUser.collectAsList()) {
      System.out.println(count.<String>getAs("name") + "\t" + count.getLong(1));
    }
    
//    System.exit(1);

    Dataset<Row> hashtag = tweets.select(explode(col("entities.hashtags.text")).as("hashtag"))
        .groupBy("hashtag").count().sort(col("count").desc()).limit(20);

    System.out.println("========================");
    System.out.println("========================");
    System.out.println("Count Hashtag");
    for (Row count : hashtag.collectAsList()) {
      System.out.println(count.<String>getAs("hashtag") + "\t" + count.<Long>getAs("count"));
    }

//    System.exit(1);
    Dataset<Row> retweetHashtag = tweets.filter(col("retweeted").equalTo(true))
        .select(explode(col("entities.hashtags.text")).as("hashtag"))
        .groupBy("hashtag").count().sort(col("count").desc()).limit(20);

    System.out.println("========================");
    System.out.println("========================");
    System.out.println("Retweet Hashtag");
    for(Row row : retweetHashtag.collectAsList()) {
      System.out.println(row.<String>getAs("hashtag") + "\t" + row.<Long>getAs("count"));
    }
  }
}
