package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.logging.Logger;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("WordRank").setMaster("local[*]");
        JavaSparkContext jsc  = new JavaSparkContext(conf);
        JavaRDD<String> initialRdd = jsc.textFile("in/word_count.text");
        JavaRDD<String> wordRdd = initialRdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCountRdd   = wordRdd.mapToPair(convertToPair());

        JavaPairRDD<String, Integer> wordCountRdd2 = wordCountRdd.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> flipped = wordCountRdd2.mapToPair(word -> new Tuple2<>(word._2, word._1));

        JavaPairRDD<Integer, String> sortedRdd = flipped.sortByKey(false);

        for (Tuple2<Integer,String> abc : sortedRdd.collect()){
            System.out.println(abc._1 + " " + abc._2) ;
        }

    }

    private static PairFunction<String, String, Integer> convertToPair(){
        return s ->  new Tuple2<String,Integer>(s,1);
    }

}

