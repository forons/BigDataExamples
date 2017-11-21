package it.unitn.spark.examples.bigdata2017;

import java.io.File;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQL {

	public static void main(String[] args) {
		// SparkSession
		Builder builder = new Builder().appName("SparkSQL Examples");
		if (new File("/Users/").exists()) {
			builder.master("local");
		}
		SparkSession spark = builder.getOrCreate();

		// Obtain JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		// Creating DataFrames
		Dataset<Row> df = spark.read().json("files/people.json");
		df.show();

		// Untyped Dataset Operations
		df.printSchema();

		df.select("name").show();

		df.select(col("name"), col("age").plus(1), col("address.city")).show();

		df.filter(col("age").gt(21)).show();

		df.groupBy("age").count().show();

		// Running SQL Queries

		df.createOrReplaceTempView("people_first");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people_first");
		sqlDF.show();

		// Global Temporary View
		// table people_first does not more exist
		spark = spark.newSession();
		try {
			df.createGlobalTempView("people_second");
		} catch (AnalysisException e) {
			e.printStackTrace();
			System.err.println("Error while registering the global view");
		}

		spark.sql("SELECT * FROM global_temp.people_second").show();
		spark.newSession().sql("SELECT * FROM global_temp.people_second").show();

		// Creating Datasets
		Person person = new Person();
		person.setName("Andy");
		person.setAge(23);
		Address address = new Address();
		address.setCity("Paris");
		address.setState("France");
		person.setAddress(address);

		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
				integerEncoder);
		transformedDS.collect();

		Dataset<Person> peopleDS = spark.read().json("files/people.json").as(personEncoder);
		peopleDS.show();

		// Interoperating with RDDs
		// Inferring the Schema Using Reflection

		JavaRDD<Person> peopleRDD = spark.read().textFile("files/people.txt").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Person p = new Person();
			p.setName(parts[0]);
			if (parts[1].isEmpty()) {
				p.setAge(0);
			} else {
				p.setAge(Integer.parseInt(parts[1].trim()));
			}
			Address a = new Address();
			a.setCity(parts[2]);
			a.setState(parts[3]);
			p.setAddress(a);
			return p;
		});

		for (Person p : peopleRDD.collect()) {
			System.out.println(p.getName() + "\t" + p.getAge() + "\t" + p.getAddress().getCity() + "\t"
					+ p.getAddress().getState());
		}
		Dataset<Row> elem = spark.createDataFrame(peopleRDD, Person.class);
		elem.printSchema();
		
		// This will NOT work, since nested JavaBeans cannot be nested
		elem.collect();
		// So we need to to something like that
		Dataset<Person> peopleDF = spark.createDataset(peopleRDD.rdd(), personEncoder);
		peopleDF.show();

		peopleDF.createOrReplaceTempView("people_third");

		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people_third WHERE age BETWEEN 13 AND 19");

		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagersDF
				.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), stringEncoder);
		teenagerNamesByIndexDF.show();

		Dataset<String> teenagerNamesByFieldDF = teenagersDF
				.map((MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"), stringEncoder);
		teenagerNamesByFieldDF.show();

		// Programmatically Specifying the Schema

		JavaRDD<String> peopleRDD2 = spark.sparkContext().textFile("files/people.txt", 1).toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age city state";
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = peopleRDD2.map((Function<String, Row>) record -> {
			String[] attributes = record.split(",");
			return RowFactory.create(attributes[0], attributes[1].trim(), attributes[2], attributes[3]);
		});

		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		peopleDataFrame.createOrReplaceTempView("people_fourth");

		Dataset<Row> results = spark.sql("SELECT name FROM people_fourth");

		Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0),
				Encoders.STRING());
		namesDS.show();

		// Data Sources

		Dataset<Row> usersDF = spark.read().load("files/users.parquet");
		usersDF.select("name", "favorite_color").write().save("files/namesAndFavColors.parquet");

		Dataset<Row> peopleDF2 = spark.read().format("json").load("files/people.json");
		peopleDF2.select("name", "age").write().format("parquet").save("files/namesAndAges.parquet");

		Dataset<Row> directSQLDF = spark.sql("SELECT * FROM parquet.`files/users.parquet`");

		// Retrieve columns
		System.out.println(Arrays.asList(peopleDF.columns()));

		// Create Row
		Row row = RowFactory.create("ciao", "hello", "hola");
		System.out.println(row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2));
	}
}
