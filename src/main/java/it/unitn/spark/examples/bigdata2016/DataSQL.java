package it.unitn.spark.examples.bigdata2016;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.Collections;

public class DataSQL {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]").getOrCreate();

		Dataset<Row> df = spark.read().format("csv").option("header", true).csv("files/people2.csv");
		df.show();
		df.printSchema();

		df.select(col("name"), col("age").plus(1)).show();

		df.filter(col("age").gt(21)).show();

		df.groupBy("age").count().show();

		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();

		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		// Encoders for most common types are provided in class Encoders
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
		Dataset<Integer> transformedDS = primitiveDS.map(x -> x + 1, Encoders.INT());
		transformedDS.collect(); // Returns [2, 3, 4]

		// DataFrames can be converted to a Dataset by providing a class.
		// Mapping based on name
		Dataset<Person> peopleDS = spark.read().format("csv").option("header", true).csv("files/people2.csv").as(personEncoder);
		peopleDS.show();

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

		// The columns of a row in the result can be accessed by field index
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(row -> "Name: " + row.getString(0), Encoders.STRING());
		teenagerNamesByIndexDF.show();

		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(row -> "Name: " + row.<String> getAs("name"), Encoders.STRING());
		teenagerNamesByFieldDF.show();
	}
}
