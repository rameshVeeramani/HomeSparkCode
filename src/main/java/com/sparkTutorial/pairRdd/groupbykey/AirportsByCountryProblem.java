package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;

public class AirportsByCountryProblem {

    public static void main(String[] args) throws Exception {

        /*
            Create a Spark program to read the airport data from in/airports.text,
            output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf config = new SparkConf().setAppName("GetAirportListBuCountry").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);

        JavaRDD<String> rddTextFile =  jsc.textFile("in/airports.text");
        JavaPairRDD<String, String> rddPair = rddTextFile.mapToPair(processEachLine());

        JavaPairRDD<String, Iterable<String>> rddGroupedByCountry = rddPair.groupByKey();

        deleteDir(new File("out/airportGroupedByCountry.txt"));

        rddGroupedByCountry.saveAsTextFile("out/airportGroupedByCountry.txt");

        rddGroupedByCountry.collect().forEach(System.out::println);

    }

    public static PairFunction<String,String,String> processEachLine() {
        return s -> new Tuple2<>(s.split(Utils.COMMA_DELIMITER)[3],
                s.split(Utils.COMMA_DELIMITER)[1]);
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
