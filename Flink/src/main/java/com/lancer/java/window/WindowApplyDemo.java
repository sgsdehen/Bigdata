package com.lancer.java.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WindowApplyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.socketTextStream("bigdata01", 9999);

        // 1000,spark,2
        SingleOutputStreamOperator<String> lineWithWaterMark = source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0].trim());
            }
        }));

        lineWithWaterMark
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value.split(",")[1].trim(), Integer.parseInt(value.split(",")[2]));
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    /**
                     * 该方法，只有窗口触发之后，才会被调用。当窗口触发后，每一个组（key相同）都会调用一次
                     * @param s 分组的key
                     * @param window 当前window对象
                     * @param input 当前窗口攒的数据（将key相同的数据放入到一个集合中）
                     * @param out 输出的数据
                     * @throws Exception
                     */
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {

                        /* 获取每个key的前Top3 */
                        /*List<Tuple2<String, Integer>> list = (ArrayList<Tuple2<String, Integer>>) input;
                        ((ArrayList<Tuple2<String, Integer>>) input).sort(new Comparator<Tuple2<String, Integer>>() {
                            @Override
                            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                                return o2.f1 - o1.f1;
                            }
                        });
                        for (int i = 0; i < Math.min(list.size(), 3); i++) {
                            out.collect(list.get(i));
                        }*/

                        System.out.println("key：" + s + "，window：" +  window);
                        int total = 0;
                        for (Tuple2<String, Integer> tp : input) {
                            total += tp.f1;
                        }
                        // 输出
                        out.collect(Tuple2.of(s, total));
                    }
                }).print();

        env.execute();
    }
}
