package com.lancer.java.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);

        env.execute("SinkApp");
    }

    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("bigdata01", 9999);

        System.out.println(source.getParallelism());

        // “>”符号前面的数字，是subtaskIndex + 1 ==> 分区号 + 1
        source.print();

        // PrintSinkOutputWriter类的open方法
        // 没有“>”符号的输出
         source.print().setParallelism(1);

         // “1>”符号前面加上test ---->  test:1> abc
        // 并行度为1   ----> test> abc
        source.print("test").setParallelism(2);
    }
}
