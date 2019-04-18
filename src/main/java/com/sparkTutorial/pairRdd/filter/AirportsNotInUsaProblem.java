package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...


        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf config = new SparkConf().setAppName("AirportNotInUSA").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);
        JavaRDD<String> inputFile = jsc.textFile("in/airports.text");
        JavaPairRDD<String, String> jpr;
        jpr = inputFile.mapToPair(retrieveKeyValue());
        JavaPairRDD<String, String> result = jpr.filter(x -> !x._2.equals("\"United States\""));
        deleteDir(new File("out/airportWithNoUSA.txt"));
        result.coalesce(1).saveAsTextFile("out/airportWithNoUSA.txt");
        result.collect().forEach(System.out::println);
        System.out.println(result.count());

        */
    }

    private static PairFunction<String,String,String> retrieveKeyValue() {
        return s -> new Tuple2<>(s.split(Utils.COMMA_DELIMITER)[1], s.split(Utils.COMMA_DELIMITER)[3]);
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
}
