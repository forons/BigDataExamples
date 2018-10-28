package it.unitn.spark.examples.bigdata2017;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class RDDMusicRecommendation {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Recommendation").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rawUserArtistData = sc
        .textFile("/Users/forons/Downloads/profiledata_06-May-2005/user_artist_data.txt");

    JavaRDD<String> rawArtistData = sc
        .textFile("/Users/forons/Downloads/profiledata_06-May-2005/artist_data.txt");
    JavaPairRDD<Integer, String> artistByID = rawArtistData.flatMapToPair(line -> {
      String[] split = line.split("\t");
      if (split.length == 2) {
        if (!split[1].isEmpty()) {
          try {
            return Collections
                .singletonList(new Tuple2<>(Integer.parseInt(split[0]), split[1].trim()))
                .iterator();
          } catch (Exception ignored) {
          }
        }
      }
      return null;
    }).filter(elem -> elem != null);

    JavaRDD<String> rawArtistAlias = sc
        .textFile("/Users/forons/Downloads/profiledata_06-May-2005/artist_alias.txt");
    Map<Integer, Integer> artistAlias = rawArtistAlias.flatMapToPair(line -> {
      String[] split = line.split("\t");
      if (!split[0].isEmpty()) {
        try {
          return Collections
              .singletonList(new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1])))
              .iterator();
        } catch (Exception ignored) {
        }
      }
      return Collections.singletonList(new Tuple2<>(-1, -1)).iterator();
    }).filter(elem -> elem != null && elem._1 != -1 && elem._2 != -1).collectAsMap();

    Broadcast<Map<Integer, Integer>> bArtistAlias = sc.broadcast(artistAlias);
    JavaRDD<Rating> trainData = rawUserArtistData.map(line -> {
      String[] split = line.split(" ");
      int userID = Integer.parseInt(split[0]);
      int artistID = Integer.parseInt(split[1]);
      int count = Integer.parseInt(split[2]);
      int finalArtistID = bArtistAlias.value().getOrDefault(artistID, artistID);
      return new Rating(userID, finalArtistID, count);
    });

    MatrixFactorizationModel model = ALS.trainImplicit(trainData.rdd(), 10, 5, 0.01, 1.0);

    JavaRDD<String[]> rawArtistsForUser = rawUserArtistData.map(line -> line.split(" "))
        .filter(x -> Integer.parseInt(x[0]) == 2093760);

    Set<Integer> existingProducts = new HashSet<>(
        rawArtistsForUser.map(x -> Integer.parseInt(x[1])).collect());

    artistByID.filter(elem -> existingProducts.contains(elem._1)).values().collect()
        .forEach(System.out::println);

    Rating[] recommendations = model.recommendProducts(2093760, 5);
    Set<Integer> recommendedProductIDs = new HashSet<>();
    for (Rating recommendation : recommendations) {
      System.out.println(recommendation);
      recommendedProductIDs.add(recommendation.product());
    }

    List<String> names = artistByID.filter(elem -> recommendedProductIDs.contains(elem._1)).values().collect();
    names.forEach(System.out::println);


  }

}
