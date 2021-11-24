package com.sparkproject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * @author : avloger
 * @date : 2021/11/23 22:10
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        //创建sparkConf
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");

        //创建javasparkcontext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //要通过并行化集合的方式创建RDD，那么调用sparkContext以及子类的parallelize（）方法
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //我们执行reduce算子操作，相当于先进行1+2 =3；3+3=6
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {


            private static final long serialVersionUID = 1L;

            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1 + v2;
            }
        });

        System.out.println("1-10的累计和=" + sum);

        sc.close();
    }

}
