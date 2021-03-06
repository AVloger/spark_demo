package com.ClickProject;

import com.sparkproject.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.awt.image.RasterOp;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : avloger
 * @date : 2021/11/22 21:23
 */
public class AccessLogDriver {
    static DBHelper db1=null;
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameProgrammatically");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 第一步，创建一个普通的RDD，但是，必须将其转换为RDD<Row>的这种格式
        JavaRDD<String> lines = sc.textFile("hdfs://s201/data/clickLog/2021/11/22");

        // 分析一下
        // 它报了一个，不能直接从String转换为Integer的一个类型转换的错误
        // 就说明什么，说明有个数据，给定义成了String类型，结果使用的时候，要用Integer类型来使用
        // 而且，错误报在sql相关的代码中
        // 所以，基本可以断定，就是说，在sql中，用到age<=18的语法，所以就强行就将age转换为Integer来使用
        // 但是，肯定是之前有些步骤，将age定义为了String
        // 所以就往前找，就找到了这里
        // 往Row中塞数据的时候，要注意，什么格式的数据，就用什么格式转换一下，再塞进去
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(String line) throws Exception {
                String itr[] = line.toString().split(" ");
                String ip = itr[0];
                String date = com.ClickProject.AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
                String url = itr[6];
                String upFlow = itr[9];
                return RowFactory.create(
                        ip,
                        date,
                        url,
                        Integer.valueOf(upFlow));
            }

        });
        // 第二步，动态构造元数据
        // 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
        // 或者是配置文件中，加载出来的，是不固定的
        // 所以特别适合用这种编程的方式，来构造元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("upflow", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 第三步，使用动态构造的元数据，将RDD转换为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD, structType);

        // 后面，就可以使用DataFrame了
        studentDF.registerTempTable("log");

        DataFrame sumFlowDF = sqlContext.sql("select ip,sum(upflow) as sum from log group by ip order by sum desc");
        db1 = new DBHelper();
        final String sql="insert into upflow(ip,sum) values(?,?) ";
        sumFlowDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row t) throws Exception {
                PreparedStatement pt = db1.conn.prepareStatement(sql);
                pt.setString(1,t.getString(0));
                pt.setString(2,String.valueOf(t.getLong(1)));
                pt.executeUpdate();
            }
        });

    }
}
