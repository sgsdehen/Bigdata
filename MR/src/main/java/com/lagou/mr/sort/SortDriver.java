package com.lagou.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Sort");

        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);

        job.setReducerClass(SortReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(SortBean.class);

        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(SortBean.class);

        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
