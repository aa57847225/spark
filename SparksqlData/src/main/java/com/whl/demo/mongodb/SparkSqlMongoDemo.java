package com.whl.demo.mongodb;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlMongoDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://115.159.201.120/saas_report.nginx_access_log")
                .config("spark.mongodb.output.uri", "mongodb://115.159.201.120/saas_report.nginx_access_log")
                .config("spark.sql.warehouse.dir","file:///D://test")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();

        //查询某些列
//        implicitDS.select("remoteAddr","request","status").show();

        //排序
        // implicitDS.sort(asc("status"), desc("bodyBytesSent")).show(10000);
//        implicitDS.sort(implicitDS.col("status").desc()).show(1000);

        // 分组
//        implicitDS.groupBy("status").count().show();
//        implicitDS.groupBy("status").agg(max("status"), sum("status")).show();

        // 条件
//        implicitDS.where("status = 200").show();
//        implicitDS.filter("status = 404").show();
//        implicitDS.where("status != 200").show();
//        implicitDS.where(implicitDS.col("status").equalTo("404")).show();

        // 并且
//        implicitDS.where(implicitDS.col("status").equalTo("404").and(implicitDS.col("bodyBytesSent").equalTo("577"))).show();

        // 或者
//        implicitDS.where(implicitDS.col("status").equalTo("200").or(implicitDS.col("bodyBytesSent").equalTo("577"))).show(1000);

        // 模糊查询
//        implicitDS.where(implicitDS.col("httpUserAgent").like("%M%")).show(1000);

        // 区间
//        implicitDS.where(implicitDS.col("status").between("400","500")).show(1000);
//        implicitDS.show(5,true);

        implicitDS.select("timeLocal").where(implicitDS.col("timeLocal").gt("23/May/2018:14:25:22 +0800")).limit(20).show();
        jsc.close();
    }
}
