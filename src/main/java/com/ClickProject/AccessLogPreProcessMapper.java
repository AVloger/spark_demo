package com.ClickProject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : avloger
 * @date : 2021/11/22 21:07
 */
public class AccessLogPreProcessMapper  extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        String value1 = value.toString();
        String itr[] = value1.toString().split(" ");
        if (itr.length < 11)
        {
            return;
        }
        String ip = itr[0];
        String date = com.ClickProject.AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
        String url = itr[6];
        String upFlow = itr[9];

        text.set(ip+","+date+","+url+","+upFlow);
        context.write(text, NullWritable.get());

    }
}
