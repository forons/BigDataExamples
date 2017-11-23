package it.unitn.spark.examples.bigdata2017;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

public class MusicRecommendation {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().master("local").appName("Recommendation System")
        .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    Dataset<Row> rawUserArtistData = spark.read().option("delimiter", " ").option("header", "false")
        .option("mode", "DROPMALFORMED")
        .csv("/Users/forons/Downloads/profiledata_06-May-2005/user_artist_data.txt");
    rawUserArtistData = rawUserArtistData.withColumnRenamed("_c0", "userID")
        .withColumnRenamed("_c1", "artistID")
        .withColumnRenamed("_c2", "playCount");

    Dataset<Row> rawArtistData = spark.read().option("delimiter", "\t").option("header", "false")
        .option("mode", "DROPMALFORMED")
        .csv("/Users/forons/Downloads/profiledata_06-May-2005/artist_data.txt");
    rawArtistData = rawArtistData.withColumnRenamed("_c0", "artistID")
        .withColumnRenamed("_c1", "name");

    Dataset<Row> artistByID = rawArtistData
        .filter(col("artistID").isNotNull().or(col("artistID").equalTo(""))
            .and(col("name").isNotNull().or(col("name").equalTo(""))))
        .withColumn("artistID", col("artistID").cast(DataTypes.IntegerType));
    artistByID.show();

    Dataset<Row> rawArtistAlias = spark.read().option("delimiter", "\t").option("header", "false")
        .option("mode", "DROPMALFORMED")
        .csv("/Users/forons/Downloads/profiledata_06-May-2005/artist_alias.txt");

    rawArtistAlias = rawArtistAlias
        .filter(col("_c0").isNotNull().or(col("_c0").equalTo(""))
            .and(col("_c1").isNotNull().or(col("_c1").equalTo(""))));

    
    List<Row> elem = rawArtistAlias.collectAsList();
    Map<Integer, Integer> map = new HashMap<>();
    		
    for(Row row : elem) {
    		map.put(Integer.parseInt(row.<String>getAs("_c0").trim()), Integer.parseInt(row.<String>getAs("_c1").trim()));
    }
    
    
    Map<Integer, Integer> artistAlias = rawArtistAlias.toJavaRDD()
        .mapToPair(row -> new Tuple2<>(Integer.parseInt(row.<String>getAs("_c0").trim()),
            Integer.parseInt(row.<String>getAs("_c1").trim()))).collectAsMap();

    int i = 0;
    for (Entry<Integer, Integer> entry : artistAlias.entrySet()) {
      if (i++ <= 20) {
        System.out.println(entry.getKey() + "\t" + entry.getValue());
      }
    }

    Broadcast<Map<Integer, Integer>> bArtistAlias = sc.broadcast(artistAlias);

    Dataset<Row> trainData = rawUserArtistData.map(new MapFunction<Row, Row>() {
      @Override
      public Row call(Row row) throws Exception {
        int userID = Integer.parseInt(row.<String>getAs("userID").trim());
        int artistID = Integer.parseInt(row.<String>getAs("artistID").trim());
        int count = Integer.parseInt(row.<String>getAs("playCount").trim());
        int finalArtistID = bArtistAlias.value().getOrDefault(artistID, artistID);

        return RowFactory.create(userID, finalArtistID, count);
      }
    }, RowEncoder.apply(new StructType(
        new StructField[]{new StructField("userID", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("artistID", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("playCount", DataTypes.IntegerType, false, Metadata.empty())})));

    ALS als = new ALS().setRank(10).setMaxIter(5).setRegParam(0.01).setUserCol("userID")
        .setItemCol("artistID").setRatingCol("playCount");
    ALSModel model = als.fit(trainData);
    
//    model.transform(dataset)

    List<Row> existingProductsList = trainData.filter(col("userID").equalTo(2093760))
        .collectAsList();
    Set<Integer> existingProducts = new HashSet<>();
    for (Row product : existingProductsList) {
      existingProducts.add(product.<Integer>getAs("artistID"));
    }

    artistByID.filter(
        (FilterFunction<Row>) value -> existingProducts
            .contains(value.<Integer>getAs("artistID"))).show();



  }
}

