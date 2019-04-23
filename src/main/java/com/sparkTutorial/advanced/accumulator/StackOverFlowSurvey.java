package com.sparkTutorial.advanced.accumulator;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

public class StackOverFlowSurvey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
        final LongAccumulator byteLength = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);
        byteLength.register(sparkContext,Option.apply("bytelength"),false);

        JavaRDD<String> responseRDD = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {

            byte[] b = response.getBytes();
            int bytelength = b.length;

            byteLength.add(bytelength);

            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);


            total.add(1);

            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            return splits[2].equals("Canada");

        });

        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
        System.out.println("byte length : " + byteLength.value());
    }
}
