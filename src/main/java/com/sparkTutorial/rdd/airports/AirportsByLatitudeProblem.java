package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import com.sun.tools.javac.util.Convert;
import com.sun.tools.javac.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.ArrayList;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

        SparkConf conf = new SparkConf().setAppName("AirportGreater40Lat").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> master = jsc.textFile("in/airports.text");

        JavaRDD<String> airportGreater40 = master.filter(line -> {
            String[] l = line.split(Utils.COMMA_DELIMITER);
            String lat = l[6];
            Double d = Double.parseDouble(lat);
            if (d > 40.0)
                return true;
            else return false;
        });

        deleteDir(new File("out/airportsGreaterThan40.txt"));

        airportGreater40.saveAsTextFile("out/airportsGreaterThan40.txt");

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
