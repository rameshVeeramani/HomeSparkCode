package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.util.parsing.combinator.testing.Str;
import sun.jvm.hotspot.asm.sparc.SPARCArgument;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        //Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF);

        SparkConf config = new SparkConf().setAppName("sumOfPRimeNumber").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);

        JavaRDD<String> file =  jsc.textFile("in/prime_nums.text");
        JavaRDD<String> i = file.flatMap( lines -> Arrays.asList(lines.split("\\s+")).iterator());

        JavaRDD<String> ne =  i.filter(n -> !n.isEmpty());

        JavaRDD<Integer> n = ne.map( inte -> Integer.valueOf(inte.trim()));

        Integer result =  n.reduce((x,y) -> x+y);
        System.out.println(result);

    }

}
/*
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");
        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);
        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(getPairFunction());
        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");

 */