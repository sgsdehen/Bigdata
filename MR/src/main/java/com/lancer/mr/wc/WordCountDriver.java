package com.lancer.mr.wc;

import com.lancer.utils.HadoopConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author lancer
 * @Date 2022/3/6 9:54 下午
 * @Description
 */
public class WordCountDriver {

    private final static Configuration conf = HadoopConfigurationUtil.getConf();

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text k = new Text();
        private static final IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(value.toString());
            while (words.hasMoreElements()) {
                // 每次都会覆盖k中的值
                k.set(words.nextToken());
                // 将数据序列化发送出去，如果没有序列化，则存的是对象的引用
                context.write(k, v);
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable value = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }

    private static class CustomPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text text, IntWritable intWritable, int numReduceTasks) {
            return (text.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        System.setProperty("HADOOP_USER_NAME", "root");

        // 设置ResourceManager地址
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "bigdata02");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // 当client访问HDFS时，NameNode只会返回与DataNode的内网ip，以下设置为true表示，可以使用hostname访问，即外网ip
        conf.set("dfs.client.use.datanode.hostname", "true");
        Job job = Job.getInstance(conf, "word count");

        // 指定程序入口
        job.setJarByClass(WordCountDriver.class);
        // 远程提交MR任务，需将任务打成Jar包再提交
        // 底层设置参数mapreduce.job.jar  --> 以前版本 mapred.jar
        job.setJar("./target/MapReduce-1.0-SNAPSHOT-jar-with-dependencies.jar");

        // 设置Map Task
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reduce Task
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Reduce Task的个数
        job.setNumReduceTasks(2);

        // 设置Combiner
        job.setCombinerClass(WordCountReducer.class);

        // 设置Partitioner
        job.setPartitionerClass(CustomPartitioner.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
