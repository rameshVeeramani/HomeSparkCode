package com.sparkTutorial.pairRdd.create;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

public class PairRddFromRegularRdd {

    public static void main(String[] args) throws Exception {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf config = new SparkConf().setAppName("PairRDD").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);
        List<String> x = Arrays.asList("Ramesh 1 ", "Babu 2", "Veeramani 3");
        JavaRDD<String> p = jsc.parallelize(x);
        JavaPairRDD<String, Integer> ret = p.mapToPair(getPairFunction());
        deleteDir(new File("out/parallel_output.txt"));
        ret.coalesce(1).saveAsTextFile("out/parallel_output.txt");
        System.out.println("done acheck the directory " + "out/parallel_output.txt");
    }

    private static PairFunction<String, String, Integer> getPairFunction() {
        return s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
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
