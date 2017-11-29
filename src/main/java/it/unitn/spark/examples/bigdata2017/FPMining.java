package it.unitn.spark.examples.bigdata2017;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class FPMining {

  public static void main(String[] args) {
    Builder builder = new Builder().appName("SparkSQL Examples");
    if (new File("/Users/").exists()) {
      builder.master("local");
    }
    SparkSession spark = builder.getOrCreate();

    Dataset<Row> tweets = spark.read().json("files/tweets.json");

    tweets.printSchema();

    tweets = tweets.select("text");

    Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
    tweets = tokenizer.transform(tweets);

//    StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol())
//        .setOutputCol("filtered");
//
//    tweets = stopWordsRemover.transform(tweets);

    // FPGrowth needs an array of distinct elements (so, no repetition)
    spark.udf().register("distinct_in_array", udf, DataTypes.createArrayType(DataTypes.StringType));

//    tweets = tweets.withColumn("distinct", callUDF("distinct_in_array", col("filtered")));
    tweets = tweets.withColumn("distinct", callUDF("distinct_in_array", col("words")));
    tweets.show();

    FPGrowthModel model = new FPGrowth()
        .setItemsCol("distinct")
        .setMinSupport(0.7)
        .setMinConfidence(0.7)
        .fit(tweets);

    // Display frequent itemsets.
    model.freqItemsets().show(false);

    // Display generated association rules.
    model.associationRules().show(false);

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(tweets).show(false);
  }

  private static UDF1<Seq<String>, Seq<String>> udf = (UDF1<Seq<String>, Seq<String>>) strings -> {
    Set<String> set = new HashSet<>(JavaConversions.seqAsJavaList(strings));
    return JavaConversions.asScalaBuffer(new ArrayList<>(set)).toSeq();
  };

}
