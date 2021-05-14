package com.mongodb.dataset;

import com.mongodb.model.ModellingDataSet;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

public class ProcessDataSet {

    public static void main(final String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Covid19Processor")
                .config("spark.mongodb.input.uri", args[0])
                .config("spark.mongodb.output.uri", args[0])
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Loading and analyzing data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        // System.out.println(rdd.count());
        System.out.println("CUSTOM READ is " + rdd.first().toJson());

        Dataset<Row> df = MongoSpark.load(jsc).toDF();

        ModellingDataSet.runSparkML(df);

    }
}