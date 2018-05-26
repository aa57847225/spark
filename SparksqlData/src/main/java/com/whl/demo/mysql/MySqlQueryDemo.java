package com.whl.demo.mysql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;

public class MySqlQueryDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.sql.warehouse.dir","file:///D://test")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://192.168.0.46:3306/test")
                .option("dbtable", "test.student")
                .option("user", "root")
                .option("password", "##!zggc5055")
                .load();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//        long start = System.currentTimeMillis();
        //System.out.println(sdf.format(new Date()));
//        jdbcDF.select("*").show();
//        long end =  System.currentTimeMillis();
//        System.out.println(end - start);

    }
}
