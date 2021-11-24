package com.sparkproject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author : avloger
 * @date : 2021/11/23 22:23
 */
public class TransforDemo {
    public static void main(String[] args) {
//        map();
//        filter();
//        flatMap();
//        groupByKey();
//        sortByKey();
//        join();
//        reduce();
        collect();
    }

    private static void collect(){
        // 创建SparkConf和JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 有一个集合，里面有1到10,10个数字，现在要对10个数字进行累加
        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbers =   sc.parallelize(numberList);

        //s使用map操作将集合中所有数字乘以2
        JavaRDD<Integer> doubleNumbers = numbers.map(

                new Function<Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }

                });

        // 不用foreach action操作，在远程集群上遍历rdd中的元素
        // 而使用collect操作，将分布在远程集群上的doubleNumbers RDD的数据拉取到本地
        // 这种方式，一般不建议使用，因为如果rdd中的数据量比较大的话，比如超过1万条
        // 那么性能会比较差，因为要从远程走大量的网络传输，将数据获取到本地
        // 此外，除了性能差，还可能在rdd中数据量特别大的情况下，发生oom异常，内存溢出
        // 因此，通常，还是推荐使用foreach action操作，来对最终的rdd元素进行处理
        List<Integer> doubleNumberList = doubleNumbers.collect();
        for(Integer num : doubleNumberList) {
            System.out.println(num);
        }

        // 关闭JavaSparkContext
        sc.close();

    }

    private static void reduce() {
        // 创建SparkConf和JavaSparkContext
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 有一个集合，里面有1到10,10个数字，现在要对10个数字进行累加
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        // 使用reduce操作对集合中的数字进行累加
        // reduce操作的原理：
        // 首先将第一个和第二个元素，传入call()方法，进行计算，会获取一个结果，比如1 + 2 = 3
        // 接着将该结果与下一个元素传入call()方法，进行计算，比如3 + 3 = 6
        // 以此类推
        // 所以reduce操作的本质，就是聚合，将多个元素聚合成一个元素
        int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }

        });

        System.out.println(sum);

        // 关闭JavaSparkContext
        sc.close();
    }

    public static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            public Integer call(Integer v1) throws Exception {

                return v1 * 2;

            }
        });
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {

            private static final long serialVersionUID = 1L;

            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

    }
    public static void filter() {
        SparkConf conf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        // 对初始RDD执行filter算子，过滤出其中的偶数
        // filter算子，传入的也是Function，其他的使用注意点，实际上和map是一样的
        // 但是，唯一的不同，就是call()方法的返回类型是Boolean
        // 每一个初始RDD中的元素，都会传入call()方法，此时你可以执行各种自定义的计算逻辑
        // 来判断这个元素是否是你想要的
        // 如果你想在新的RDD中保留这个元素，那么就返回true；否则，不想保留这个元素，返回false
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(

                new Function<Integer, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    // 在这里，1到10，都会传入进来
                    // 但是根据我们的逻辑，只有2,4,6,8,10这几个偶数，会返回true
                    // 所以，只有偶数会保留下来，放在新的RDD中
                    public Boolean call(Integer v1) throws Exception {
                        return v1 % 2 == 0;
                    }

                });

        // 打印新的RDD
        evenNumberRDD.foreach(new VoidFunction<Integer>() {

            private static final long serialVersionUID = 1L;
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }

        });

        // 关闭JavaSparkContext
        sc.close();

    }
    private static void flatMap() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("flatMap")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构造集合
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        // 并行化集合，创建RDD
        JavaRDD<String> lines = sc.parallelize(lineList);

        // 对RDD执行flatMap算子，将每一行文本，拆分为多个单词
        // flatMap算子，在java中，接收的参数是FlatMapFunction
        // 我们需要自己定义FlatMapFunction的第二个泛型类型，即，代表了返回的新元素的类型
        // call()方法，返回的类型，不是U，而是Iterable<U>，这里的U也与第二个泛型类型相同
        // flatMap其实就是，接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，可以返回多个元素
        // 多个元素，即封装在Iterable集合中，可以使用ArrayList等集合
        // 新的RDD中，即封装了所有的新元素；也就是说，新的RDD的大小一定是 >= 原始RDD的大小
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            // 在这里会，比如，传入第一行，hello you
            // 返回的是一个Iterable<String>(hello, you)
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }

        });

        // 打印新的RDD
        words.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
        // 关闭JavaSparkContext
        sc.close();
    }
    private static void groupByKey() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 65));

        // 并行化集合，创建JavaPairRDD
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);

        // 针对scores RDD，执行groupByKey算子，对每个班级的成绩进行分组
        // groupByKey算子，返回的还是JavaPairRDD
        // 但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成Iterable这种集合类型
        // 也就是说，按照了key进行分组，那么每个key可能都会有多个value，此时多个value聚合成了Iterable
        // 那么接下来，我们是不是就可以通过groupedScores这种JavaPairRDD，很方便地处理某个分组内的数据
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

        // 打印groupedScores RDD
        groupedScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, Iterable<Integer>> t)
                    throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> ite = t._2.iterator();
                while(ite.hasNext()) {
                    System.out.println(ite.next());
                }
                System.out.println("==============================");
            }

        });

        // 关闭JavaSparkContext
        sc.close();
    }
    private static void sortByKey() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("sortByKey")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(65, "leo"),
                new Tuple2<Integer, String>(50, "tom"),
                new Tuple2<Integer, String>(100, "marry"),
                new Tuple2<Integer, String>(80, "jack"));

        // 并行化集合，创建RDD
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);

        // 对scores RDD执行sortByKey算子
        // sortByKey其实就是根据key进行排序，可以手动指定升序，或者降序
        // 返回的，还是JavaPairRDD，其中的元素内容，都是和原始的RDD一模一样的
        // 但是就是RDD中的元素的顺序，不同了
        JavaPairRDD<Integer, String> sortedScores = scores.sortByKey(false);

        // 打印sortedScored RDD
        sortedScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }

        });

        // 关闭JavaSparkContext
        sc.close();
    }
    private static void join() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("join")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60));

        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        // 使用join算子关联两个RDD
        // join以后，还是会根据key进行join，并返回JavaPairRDD
        // 但是JavaPairRDD的第一个泛型类型是之前两个JavaPairRDD的key的类型，因为是通过key进行join的
        // 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
        // join，就返回的RDD的每一个元素，就是通过key join上的一个pair
        // 什么意思呢？比如有(1, 1) (1, 2) (1, 3)的一个RDD
        // 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
        // join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);

        // 打印studnetScores RDD
        studentScores.foreach(

                new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

                    private static final long serialVersionUID = 1L;

                    public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                            throws Exception {
                        System.out.println("student id: " + t._1);
                        System.out.println("student name: " + t._2._1);
                        System.out.println("student score: " + t._2._2);
                        System.out.println("===============================");
                    }

                });

        // 关闭JavaSparkContext
        sc.close();
    }

}
