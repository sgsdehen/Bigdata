package com.lancer.mr.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author lancer
 * @Date 2022/3/7 8:15 下午
 * @Description
 */
public class MapJoinDriver {
    private static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // <positionId, positionName>
        private static final Map<String, String> position = new HashMap<>();

        private final Text t = new Text();
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();

            FileSystem fs;
            try {
                fs = FileSystem.get(new URI("hdfs://bigdata01:9000"), new Configuration());
                FSDataInputStream dis = fs.open(new Path(cacheFiles[0]));
                BufferedReader br = new BufferedReader(new InputStreamReader(dis));

                // BufferedReader br = new BufferedReader(new FileReader(new File(cacheFiles[0])));
                String line;
                while (StringUtils.isNotEmpty(line = br.readLine())) {
                    String[] splits = line.split("\\t");
                    position.put(splits[0], splits[1]);
                }
                br.close();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\t");
            String k = splits[1];
            String v = position.getOrDefault(k, "");
            if (StringUtils.isNotBlank(v)) {
                t.set(splits[0]+ "\t" + splits[1] + "\t" + v + "\t" + splits[2]);
                context.write(t, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mapJoin");

        job.setJarByClass(MapJoinDriver.class);

        // job.addCacheFile(new URI("file:///Users/lancer/IdeaProjects/MapReduce/data/position.txt"));
        job.addCacheFile(new URI("hdfs://bigdata01:9000/position.txt"));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 不需要reduce task
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
