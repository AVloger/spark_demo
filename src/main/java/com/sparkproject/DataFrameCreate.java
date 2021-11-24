package com.sparkproject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author : avloger
 * @date : 2021/11/23 23:21
 */
public class DataFrameCreate {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("C:\\Users\\ASUS\\Desktop\\spark.json");

        df.printSchema();
        //查询某列所有数据
        df.select("name").show();
        //查询某几个列所有数据并对列进行计算
        df.select(df.col("name"),df.col("age").plus(1)).show();
        //过滤
        df.filter(df.col("age").gt(18)).show();
        //按照组进行统计
        df.groupBy(df.col("age")).count().show();

//        df.show();

    }

}
