package com.mongodb.spark_examples;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class WriteToMongoDB {

    public static final String uri = "mongodb+srv://myadmin:myadmin@cluster0.q8vwk.mongodb.net/mongo_spark.spark";


    public static void main(final String[] args) throws InterruptedException {

        dropDatabase(uri);

        // Add Sample Data
        List<String> sample_data = new ArrayList<>();/*asList(
                "{'name': 'Bilbo Baggins', 'age': 50}",
                "{'name': 'Gandalf', 'age': 1000}",
                "{'name': 'Thorin', 'age': 195}",
                "{'name': 'Balin', 'age': 178}",
                "{'name': 'Kíli', 'age': 77}",
                "{'name': 'Dwalin', 'age': 169}",
                "{'name': 'Óin', 'age': 167}",
                "{'name': 'Glóin', 'age': 158}",
                "{'name': 'Fíli', 'age': 82}",
                "{'name': 'Bombur'}"
        );*/
        long start1 = System.currentTimeMillis();
        for(int i=1;i<1000000;i++){

            sample_data.add( "{'name': 'Bilbo Baggins', 'age': "+i+"}");
        }
        long end1 = System.currentTimeMillis();
        long elapsedTime1 = end1 - start1/1000;
        System.out.println("Time taken to loop 1 million records is "+elapsedTime1);

        SparkSession spark = SparkSession.builder().appName("MongoSparkConnectorIntro").master("local")
                .config("spark.mongodb.output.uri", uri)
                .config("spark.mongodb.input.uri",uri)
                .config("spark.mongodb.input.database", "ConsultScheduler")
                .config("spark.mongodb.input.collection", "config")
                .config("spark.driver.memory", "4g")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
               .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Create a RDD of 10 documents
        JavaRDD<Document> sparkDocuments = jsc.parallelize(sample_data).map(new Function<String, Document>() {
                    @Override
                    public Document call(final String sample_data) throws Exception {
                        return Document.parse(sample_data);
                    }
                });

        /*Start Example: Save data from RDD to MongoDB*****************/
        long start = System.currentTimeMillis();
        MongoSpark.save(sparkDocuments);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        System.out.println("Time taken to insert 1 million records is "+(elapsedTime / 1000) % 60);
        //Time taken to insert 1 million records is 1618337673643
        /*End Example**************************************************/

        jsc.close();

    }

    private static void dropDatabase(final String connectionString) {
        ConnectionString uri = new ConnectionString(connectionString);
        MongoClients.create(uri).getDatabase(uri.getDatabase()).drop();
    }

}