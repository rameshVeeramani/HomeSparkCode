package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

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

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.

*/

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf config = new SparkConf().setAppName("AverageAreaCost").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);
        JavaRDD<String> textRdd = jsc.textFile("in/RealEstate.csv");

        JavaRDD<String>  tt = textRdd.filter((line -> !line.contains("Bedrooms")));

        JavaPairRDD<Integer, AvgCount> AreaKeyRdd = tt.mapToPair(line -> new Tuple2<>(
                Integer.valueOf(line.split(",")[3])
                ,new AvgCount(1, Double.valueOf(line.split(",")[2]))));

        JavaPairRDD<Integer, AvgCount> average = AreaKeyRdd.reduceByKey((x, y) ->
                new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));

        JavaPairRDD<Integer, AvgCount> a = AreaKeyRdd.reduceByKey((x, y) ->
                new AvgCount(x.getCount()+y.getCount(), x.getTotal()+y.getTotal()));

        System.out.println("Average");
        for (Map.Entry<Integer, AvgCount> housePriceTotalPair : a.collectAsMap().entrySet())
        {
            //AvgCount xx = housePriceTotalPair.getValue();
            System.out.println(housePriceTotalPair.getKey() + " : " + housePriceTotalPair.getValue());
        }

        JavaPairRDD<Integer, Double> xx = a.mapValues(x -> x.getTotal() / x.getCount());

        JavaPairRDD<Integer, Double> sorted = xx.sortByKey(true);

        for(Tuple2 abc  : sorted.collect()){
            System.out.println( abc._1()  + " " + abc._2() ) ;
        }
        /*
        for( Map.Entry<Integer, Double> abc : sorted.collectAsMap().entrySet()){
            System.out.println( abc.getKey() + " " + abc.getValue()) ;
        }
        */
    }
}
