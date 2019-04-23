package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class HousePriceProblem {
        /* Create a Spark program to read the house data from in/RealEstate.csv,
           group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

        +----------------+-----------------+----------+
        |        Location| avg(Price SQ Ft)|max(Price)|
        +----------------+-----------------+----------+
        |          Oceano|           1145.0|   1195000|
        |         Bradley|            606.0|   1600000|
        | San Luis Obispo|            459.0|   2369000|
        |      Santa Ynez|            391.4|   1395000|
        |         Cayucos|            387.0|   1500000|
        |.............................................|
        |.............................................|
        |.............................................|

         */

        public static void main(String[] args) {

                Logger.getLogger("org").setLevel(Level.ERROR);
                SparkSession sparkSession = SparkSession.builder().appName("HomePrice").master("local[*]").getOrCreate();
                //SparkSession sparkSession = SparkSession.builder().appName("StackOverFlowSurvey").master("local[9]").getOrCreate();
                DataFrameReader dataFrameReader = sparkSession.read();
                Dataset<Row> datasets = dataFrameReader.option("header", true).csv("in/RealEstate.csv");

                datasets.printSchema();
                RelationalGroupedDataset groupedByLocation = datasets.groupBy(col("Location"));
                groupedByLocation.count().orderBy(col("count").desc()).show();
                Dataset<Row> withInteger = datasets
                        .withColumn("IPricePersqft", col("Price SQ Ft").cast("Double"))
                        .withColumn("IPrice",col("Price").cast("Integer"));

                datasets.show();
                withInteger.show();




                Dataset<Row> ppsqft  = withInteger.groupBy(col("Location")).agg( avg("IPricePersqft"),max("IPricePersqft"));
                ppsqft.sort(col("avg(IPricePersqft)").desc()).show();

                Dataset<Row> max =withInteger.groupBy(col("Location")).agg( avg("IPrice"),max("IPrice"));
                max.sort(col("max(IPrice)").desc()).show();

        }



}
