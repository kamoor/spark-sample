package com.kamoor.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by kamoorr on 12/27/15.
 * This code will run on EMR cluster and connect to Hive Meta data
 *
 * spark-submit --name "My app" --master local[4] --conf spark.shuffle.spill=false --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --class com.kamoor.spark.java.HiveCount  spark-samples-1.0.0-SNAPSHOT.jar
 * 
 */
public class HiveCount {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(ctx);

        HiveContext hiveContext = new HiveContext(ctx.sc());

        DataFrame countFrame = hiveContext.sql("select count(*) from test ");

        List<String> counts = countFrame.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Count: " + row.getString(0);
            }
        }).collect();
        for (String c: counts) {
            System.out.println(c);
        }

        ctx.stop();
    }
}
