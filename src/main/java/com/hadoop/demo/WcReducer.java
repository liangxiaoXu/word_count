package com.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xuliangxiao on 2017/6/23 17:31
 */
public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum = sum + i.get();
        }

        System.out.println("WcReducer key=" + key + ", num=" + sum);
        context.write(key, new IntWritable(sum));
    }
}
