package com.mongodb.spark_examples;

import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

import java.io.Serializable;

public class SQLDataSetsMongoDB {


        public static void main(final String[] args) throws InterruptedException {

            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("MongoSparkConnectorIntro")
                    .config("spark.mongodb.input.uri", "mongodb+srv://myadmin:myadmin@cluster0.q8vwk.mongodb.net/sample_mflix.spark")
                    .config("spark.mongodb.output.uri", "mongodb+srv://myadmin:myadmin@cluster0.q8vwk.mongodb.net/sample_mflix.spark")
                    .getOrCreate();

            // Create a JavaSparkContext using the SparkSession's SparkContext object
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

            // Load data and infer schema, disregard toDF() name as it returns Dataset
            Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
            implicitDS.printSchema();
            implicitDS.show();

            // Load data with explicit schema
            Dataset<Character> explicitDS = MongoSpark.load(jsc).toDS(Character.class);
            explicitDS.printSchema();

            // Create the temp view and execute the query
            explicitDS.createOrReplaceTempView("characters");
            Dataset<Row> centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100");
            centenarians.show();

            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            // Write the data to the "hundredClub" collection
            MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

            // Load the data from the "hundredClub" collection
            MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("collection", "hundredClub"), Character.class).show();

            jsc.close();

        }
}