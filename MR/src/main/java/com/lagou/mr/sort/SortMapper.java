package com.lagou.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {

    private SortBean sortBean = new SortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行文本转为字符串，切分
        String[] splits = value.toString().split("\t");
        sortBean.setDeviceId(splits[0]);
        sortBean.setSelfDuration(Long.parseLong(splits[1]));
        sortBean.setThirdPartDuration(Long.parseLong(splits[2]));
        sortBean.setSumDuration(Long.parseLong(splits[4]));

        context.write(sortBean, NullWritable.get());
    }
}
