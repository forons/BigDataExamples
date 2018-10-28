package it.unitn.spark.examples.bigdata2017;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

public class AnomalyDetection {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Recommendation").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rawData = sc.textFile("/Users/forons/Downloads/kddcup.data");

    Map<String, Long> elements = rawData.map(line -> line.split(",")[line.split(",").length - 1]).countByValue();
    for (Entry<String, Long> entry : elements.entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }

    JavaPairRDD<String, Vector> labelsAndData = rawData.mapToPair(line -> {
      List<String> split = Arrays.asList(line.split(","));
      split.remove(1);
      split.remove(3);
      String label = split.remove(split.size() - 1);
      double[] buffer = new double[split.size()];
      for(int i = 0; i < buffer.length; i++) {
        buffer[i] = Double.parseDouble(split.get(i));
      }
      Vector vector = Vectors.dense(buffer);
      return new Tuple2<>(label, vector);
    });

    JavaRDD<Vector> data = labelsAndData.values().cache();

    KMeans kmeans = new KMeans();
    KMeansModel model = kmeans.run(data.rdd());
    for(Vector vector : model.clusterCenters()) {
      System.out.println(vector);
    }

    Map<Tuple2<Integer, String>, Long> clusterLabelCount = labelsAndData.mapToPair(elem -> {
      int cluster = model.predict(elem._2);
      return new Tuple2<>(cluster, elem._1);
    }).countByValue();

//    clusterLabelCoun




  }

}
