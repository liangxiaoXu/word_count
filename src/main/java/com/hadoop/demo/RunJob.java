package com.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by xuliangxiao on 2017/6/23 17:33
 */
public class RunJob {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //设置hdfs的通讯地址
//        config.set("fs.defaultFS", "hdfs://node1:8020");
        //设置RN的主机
//        config.set("yarn.resourcemanager.hostname", "node1");

        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);

            job.setJobName("wc");

            job.setMapperClass(WcMapper.class);
            job.setReducerClass(WcReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            FileInputFormat.addInputPath(job, new Path("/usr/input/wc.txt"));
//            Path outpath = new Path("/usr/output/wc");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outpath = new Path(args[1]);

            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
