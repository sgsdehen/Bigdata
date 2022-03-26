package com.lagou.mr.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {

    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 为了避免前面compareTo方法导致总流量相等被当成对象相等，而合并了key，所以遍历values获取每个key(bean对象)
        for (NullWritable value : values) { // 遍历value的同时，key也会随着遍历
            context.write(key, value);
        }
    }
}
