package com.lagou.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    // key：reduce方法的key是一组相同key的kv的第一个key
    // values：一组相同key的kv对中v的集合
    // 对于如何判断key是否相同，自定义对象是需要我们指定一个规则，这个规则通过groupingComparator来指定
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

            context.write(key, NullWritable.get());
        }
    }
}
