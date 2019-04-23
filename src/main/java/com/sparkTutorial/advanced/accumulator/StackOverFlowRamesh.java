package com.sparkTutorial.advanced.accumulator;

import com.sparkTutorial.rdd.commons.Utils;
import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;


public class StackOverFlowRamesh {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sss").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<String> abc = jsc.textFile("in/2016-stack-overflow-survey-responses.csv");
        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator m = new LongAccumulator();
        total.register(sc, Option.apply("total"), false);
        m.register(sc, Option.apply("canada"), false);
        JavaRDD<String> canada = abc.filter(
                x -> {
                    String[] ss = x.split(Utils.COMMA_DELIMITER, -2);
                    total.add(1);
                    if (ss[14].isEmpty())
                        m.add(1);
                    return (ss[2].equals("Canada"));
                }
        );
        System.out.println("TOTAL RECORDS " +  abc.count());
        System.out.println("NUMBER OF CANADA RECORDS "+canada.count());
        System.out.println("NUMBER OF RECORDS WITH NO SALARAY USING ACCUMLATOR " + m.value());
        System.out.println("NUMBER OF RECORDS USING ACCUMLATOR  "+total.value());
    }
}
