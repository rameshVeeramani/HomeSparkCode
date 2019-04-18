package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.ArrayList;
import java.util.logging.Logger;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */


        SparkConf conf = new SparkConf().setAppName("AirportApplication").setMaster("local[*]");
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> first = jsc.textFile("in/airports.text");
        JavaRDD<String> second = first.filter(x -> x.split(Utils.COMMA_DELIMITER)[3].equals("\"India\""));

        JavaRDD<String> third = second.map(l -> {
            String[] splits = l.split(",");
            return splits[1] + "," + splits[2];
        });

        String path = "out/airports_in_usa.txt";
        deleteDir(new File(path));
        third.saveAsTextFile("out/airports_in_usa.txt");
        third.collect().forEach(System.out::println);
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
