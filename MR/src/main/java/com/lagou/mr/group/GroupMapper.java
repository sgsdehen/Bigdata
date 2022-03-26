package com.lagou.mr.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        // 订单id与金额封装为OrderBean
        orderBean.setOrderId(fields[0]);
        orderBean.setPrice(Double.parseDouble(fields[fields.length - 1]));
        context.write(orderBean, NullWritable.get());
    }
}
