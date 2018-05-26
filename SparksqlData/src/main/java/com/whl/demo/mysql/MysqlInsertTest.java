package com.whl.demo.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MysqlInsertTest {

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local"));

        SQLContext sqlContext = new SQLContext(sparkContext);
        //写入的数据内容
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("tom 15 男","jack 6 女","alex 7 女"));

        //数据库内容
        String url = "jdbc:mysql://192.168.0.46:3306/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","##!zggc5055");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");


        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         * */
        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(splited[0],Integer.valueOf(splited[1]),splited[2]);
            }
        });


        /**
         * 第二步：动态构造DataFrame的元数据。
         * */
        List structFields = new ArrayList();
//        structFields.add(DataTypes.createStructField("s_no",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("sname",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("sage",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("ssex",DataTypes.StringType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        Dataset personsDF = sqlContext.createDataFrame(personsRDD,structType);

        /**
         * 第四步：将数据写入到person表中
         */
        for(int i=0;i<10;i++){
            personsDF.write().mode("append").jdbc(url,"student",connectionProperties);
        }


        //停止SparkContext
        sparkContext.stop();
    }
}
