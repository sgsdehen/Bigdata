package com.lancer.java.source;

import com.lancer.java.transformations.TransformationApp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 并行度通过ExecutionConfig类来设定
 * <p>
 * 通过run方法Starts the source ---> 源操作（每个并行度执行一次）
 * <p>
 * 通过open方法，在每个Function使用前做操作  ---> 一个并行度执行一次
 */
public class SourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test08(env);

        env.execute("SourceApp");
    }

    public static void test08(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.fromElements(String.class, "a b c", "d e f");

        System.out.println(source.getParallelism()); // 1

        source
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String s : value.split(" ")) {
                            out.collect(s);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();
    }

    // parallel的Source
    public static void test07(StreamExecutionEnvironment env) {
        // 自定义 parallel的source
        class AccessSourceV2 extends RichParallelSourceFunction<TransformationApp.Access> {

            private boolean running = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("~~~~~~~~open~~~~~~~~");
            }

            @Override
            public void run(SourceContext<TransformationApp.Access> ctx) throws Exception {
                String[] domains = new String[]{"lancer.com", "a.com", "b.com"};
                Random random = new Random();
                while (running) {
                    for (int i = 0; i < 10; i++) {
                        TransformationApp.Access access = new TransformationApp.Access();
                        access.setTime(1234567L);
                        access.setDomain(domains[random.nextInt(domains.length)]);
                        access.setTraffic(random.nextDouble() + 1000);

                        ctx.collect(access);
                    }
                    Thread.sleep(5000); // 每5s发送10条数据出去
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }

        DataStreamSource<TransformationApp.Access> source = env.addSource(new AccessSourceV2());
        System.out.println(source.getParallelism()); // 16
        source.print(); // 16 * 10条数据/s
    }

    /*
     * non-parallel的Source
     * */
    public static void test06(StreamExecutionEnvironment env) {
        // 自定义non-parallel的source
        class AccessSourceV1 extends RichSourceFunction<TransformationApp.Access> {

            private boolean running = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("~~~~~~~~open~~~~~~~~");
            }

            @Override
            public void run(SourceContext<TransformationApp.Access> sourceContext) throws Exception {

                String[] domains = new String[]{"lancer.com", "a.com", "b.com"};
                Random random = new Random();
                while (running) {
                    for (int i = 0; i < 10; i++) {
                        TransformationApp.Access access = new TransformationApp.Access();
                        access.setTime(1234567L);
                        access.setDomain(domains[random.nextInt(domains.length)]);
                        access.setTraffic(random.nextDouble() + 1000);

                        sourceContext.collect(access);
                    }
                    Thread.sleep(5000); // 每5s发送10条数据出去
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }
        DataStreamSource<TransformationApp.Access> source = env.addSource(new AccessSourceV1());
        System.out.println(source.getParallelism()); // 1
        source.print(); // 1 * 10条数据/s
    }

    // fromCollection()
    public static void test05(StreamExecutionEnvironment env) {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);

        DataStreamSource<Integer> collection = env
                .fromCollection(list);// .setParallelism(3);
        // The maximum parallelism of non parallel operator must be 1.
        // System.out.println(collection.getParallelism()); // 1

        collection
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer * 2;
                    }
                })
                .print();
    }

    /*
     * 通过读取ExecutionConfig中的parallelism，通过FileInputFormat读取文件，按parallelism数逻辑切分?
     * */
    public static void test04(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("./data/access.log");
        System.out.println(source.getParallelism()); // 16  ?
    }

    // Kafka
    public static void test03(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092,bigdata04:9092,bigdata05:9092");
        properties.setProperty("group.id", "test");
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("flinkTopic", new SimpleStringSchema(), properties));

        System.out.println("streamSource's parallelism：" + streamSource.getParallelism());
        streamSource.print();
    }

    // fromParallelCollection
    public static void test02(StreamExecutionEnvironment env) {
        // 调用AbstractRichFunction的open方法，从RuntimeContext中获取task数量（并行度），然后返回一个切分后的迭代器iterator，通过ExecutionConfig中的parallelism来设定调用次数
        /*
        * public void open(Configuration parameters) throws Exception {
                int numberOfSubTasks = this.getRuntimeContext().getNumberOfParallelSubtasks(); // 获取task的数量
                int indexofThisSubTask = this.getRuntimeContext().getIndexOfThisSubtask();  // 获取该task的下标
                this.iterator = this.fullIterator.split(numberOfSubTasks)[indexofThisSubTask]; // 返回迭代器数组中，task下标处的迭代器
                this.isRunning = true;
         }
        * */
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("fromParallelCollection's parallelism：" + source.getParallelism()); // 16

        SingleOutputStreamOperator<Long> filterStream = source.filter(s -> s >= 5);

        System.out.println("filterStream's parallelism：" + filterStream.getParallelism());
    }

    // socketTextStream
    public static void test01(StreamExecutionEnvironment env) {
        env.setParallelism(5); // 设置全局并行度

        // 底层都是使用StreamExecutionEnvironment.addSource(new xxxFunction() ---> who implements xxxSourceFunction<T>)
        // SourceFunction并行度为1(isParallel=false)，ParallelSourceFunction和RichParallelSourceFunction，可以指定并行度
        // boolean isParallel = function instanceof ParallelSourceFunction;
        DataStreamSource<String> source = env.socketTextStream("bigdata01", 9999); // .setParallelism(2); 报错，The maximum parallelism of non parallel operator must be 1.
        System.out.println("socketTextStream's parallelism：" + source.getParallelism()); // 1

        // 接收socket过来的数据，一行一个单词，把hello过滤掉
        SingleOutputStreamOperator<String> filterStream = source
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return !s.equalsIgnoreCase("hello");
                    }
                }).setParallelism(1);

        System.out.println("filterStream's parallelism：" + filterStream.getParallelism());
    }
}
