package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */


        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf config = new SparkConf().setAppName("NasaLogFiles").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);

        JavaRDD<String> julyFile = jsc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFile = jsc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> aggregratedFile = julyFile.union(augustFile);
        JavaRDD<String> removedHeader = aggregratedFile.filter(l -> !(l.contains("bytes") && l.startsWith("host")));
        JavaRDD<String> result = removedHeader.sample(true, 0.001);
        result.saveAsTextFile("out/nasa_union_output.txt");
    }
}
