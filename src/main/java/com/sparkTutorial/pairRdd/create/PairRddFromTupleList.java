package com.sparkTutorial.pairRdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PairRddFromTupleList {

    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> x = Arrays.asList(
                new Tuple2<String, Integer>("ramesh1", 123),
                new Tuple2<String, Integer>("ramesh2", 13),
                new Tuple2<String, Integer>("ramesh3", 13),
                new Tuple2<String, Integer>("ramesh4", 1)
        );
        JavaPairRDD<String, Integer> pairrdd = sc.parallelizePairs(x);
        JavaPairRDD<String, Integer> rdd = pairrdd.coalesce(1);
        deleteDir(new File("out/x.txt"));
        rdd.saveAsTextFile("out/x.txt");
        System.out.println("Done.");
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
