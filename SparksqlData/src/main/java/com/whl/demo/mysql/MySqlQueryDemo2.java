package com.whl.demo.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class MySqlQueryDemo2 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.sql.warehouse.dir","file:///D://test")
                .master("local")
                .getOrCreate();

        String url = "jdbc:mysql://192.168.0.46:3306/test";
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","##!zggc5055");

        Dataset<Row> studentDF = spark.read().jdbc(url,"student",properties);
        Dataset<Row> studentinfoDF = spark.read().jdbc(url,"student_info",properties);
        studentDF.registerTempTable("student");
        studentinfoDF.registerTempTable("student_info");

        // 表关联查询两种方式
//        spark.sql("select * from student s left join student_info info on s.s_no = info.stu_id").show(10);
//        studentDF.join(studentinfoDF,studentinfoDF.col("stu_id").equalTo(studentDF.col("s_no")),"left").show();

//        studentDF.show();
        
    }
}
