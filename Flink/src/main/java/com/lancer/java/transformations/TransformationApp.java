package com.lancer.java.transformations;

import lombok.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class TransformationApp {
    // Flink中的POJO要求实体类为public
    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    @ToString
    public static class Access {
        private Long time;
        private String domain;
        private Double traffic;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        transform_reduce(env);

        env.execute("TransformationApp");
    }

    // 测流输出
    public static void transform_outputTag(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Access> srcDataStream = env
                .readTextFile("./data/access.log")

                // transformation
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Long time = Long.parseLong(splits[0].trim());
                        String domain = splits[1].trim();
                        Double traffic = Double.parseDouble(splits[2].trim());
                        return new Access(time, domain, traffic);
                    }
                });

        // OutputTag<Access> outputTag = new OutputTag<>("isBad", TypeInformation.of(Access.class));
        OutputTag<Access> outputTag = new OutputTag<Access>("isBad") {
        };

        SingleOutputStreamOperator<Access> resultStream =
                srcDataStream.process(new ProcessFunction<Access, Access>() {
                    // 以下方法，一个元素调用一次
                    @Override
                    public void processElement(Access value, Context ctx, Collector<Access> out) throws Exception {
                        boolean isGood = value.getTraffic() > 2000;
                        if (isGood) {
                            out.collect(value);
                        } else {
                            ctx.output(outputTag, value);
                        }
                    }
                });

        resultStream
                .map(new MapFunction<Access, Tuple2<String, Access>>() {
                    @Override
                    public Tuple2<String, Access> map(Access value) throws Exception {
                        return Tuple2.of("hello--------------->", value);
                    }
                }).print("isGood");

        resultStream.getSideOutput(outputTag).print("isBad");
    }

    // 使用侧输出流来代替split + select
    /*public static void transform_split_select(StreamExecutionEnvironment env) {
        SplitStream<Access> splitStream = env
                .readTextFile("./data/access.log")

                // transformation
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Long time = Long.parseLong(splits[0].trim());
                        String domain = splits[1].trim();
                        Double traffic = Double.parseDouble(splits[2].trim());
                        return new Access(time, domain, traffic);
                    }
                })
                .split(new OutputSelector<Access>() {
                    @Override
                    public Iterable<String> select(Access value) {
                        boolean isGood = value.getTraffic() > 2000;
                        if (isGood) {
                            return () -> {
                                List<String> list = new ArrayList<>();
                                list.add("isGood");
                                return list.iterator();
                            };
                        } else {
                            return () -> {
                                String[] arr = {"isBad"};
                                return Arrays.stream(arr).iterator();
                            };
                        }
                    }
                });

        splitStream.select("isGood").print("isGood");

        splitStream.select("isBad").print("isBad");
    }*/

    // 在实际生产环境中应该尽量避免在一个无限流上使用 Aggregations
    public static void transform_max_maxBy(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        // source
        env
                .readTextFile("./data/access.log")

                // transformation
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Long time = Long.parseLong(splits[0].trim());
                        String domain = splits[1].trim();
                        Double traffic = Double.parseDouble(splits[2].trim());
                        return new Access(time, domain, traffic);
                    }
                })
                /*.keyBy(new KeySelector<Access, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Access value) throws Exception {
                        return Tuple2.of(value.getTime(), value.getDomain());
                    }
                })
                // max 会替换指定字段的最大值，但是其余字段不会更新，都是旧值，然后输出
                // max 会返回最大值，同时将最大值替换小的那个，跟其他字段无关
                // maxBy会返回最大值所在的那一条信息
                // 都是获取最大值，max不更新其他元素，将指定字段的值更新后，连同旧数据一起返回。maxBy直接将最大值的那条数据返回
                .max("traffic")*/
                .keyBy(new KeySelector<Access, Long>() {
                    @Override
                    public Long getKey(Access value) throws Exception {
                        return value.getTime();
                    }
                })
                .maxBy("traffic")
                // sink
                .print();
    }

    public static void transform_coFlatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source1 = env.fromElements(String.class, "a b c", "d e f");
        DataStreamSource<String> source2 = env.fromElements(String.class, "a,b,c", "d,e,f");

        ConnectedStreams<String, String> connectStream = source1.connect(source2);

        SingleOutputStreamOperator<String> flatMapStream = connectStream.flatMap(new CoFlatMapFunction<String, String, String>() {
            // 第一条流
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }

            // 第二条流
            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        });

        flatMapStream.print();
    }

    // CoMap和CoFlatMap是专门用来将ConnectedStreams -> DataStream
    public static void transform_coMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source1 = env.socketTextStream("bigdata01", 9998);
        SingleOutputStreamOperator<Integer> source2 = env
                .socketTextStream("bigdata01", 9999)
                .filter(NumberUtils::isNumber)
                .map(Integer::parseInt);

        ConnectedStreams<String, Integer> connectStream = source1.connect(source2);

        SingleOutputStreamOperator<String> mapStream = connectStream.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 10 + "";
            }
        });

        mapStream.print();
    }

    // 将两流合并
    public static void transform_connect(StreamExecutionEnvironment env) {
        SourceFunction<Access> source = new RichSourceFunction<Access>() {

            private boolean running = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("~~~~~~open~~~~~~");
            }

            @Override
            public void run(SourceContext<Access> ctx) throws Exception {
                String[] domains = new String[]{"lancer.com", "a.com", "b.com"};
                Random random = new Random();
                while (running) {
                    for (int i = 0; i < 10; i++) {
                        Access access = new Access();
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
        };
        DataStreamSource<Access> source1 = env.addSource(source);
        DataStreamSource<Access> source2 = env.addSource(source);

        // 为了方便区分，给source2处理一下
        SingleOutputStreamOperator<Tuple2<String, Access>> newSource2 = source2.map(new MapFunction<Access, Tuple2<String, Access>>() {
            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
                return Tuple2.of("lancer", value);
            }
        });

        // source1和source2的类型可以不一样，双流合并，调用transform算子才会合并，更加灵活，
        // connect经常被应用在对一个数据流使用另外一个流进行控制处理的场景上
        ConnectedStreams<Access, Tuple2<String, Access>> connectStream = source1.connect(newSource2);

        // connectStream.print(); 报错

        // 对两条流操作后，合并成一条
        SingleOutputStreamOperator<String> mapStream = connectStream.map(new CoMapFunction<Access, Tuple2<String, Access>, String>() {
            // 第一条流
            @Override
            public String map1(Access value) throws Exception {
                return value.toString();
            }

            // 第二条流
            @Override
            public String map2(Tuple2<String, Access> value) throws Exception {
                return value.f0 + "====>" + value.f1.toString();
            }
        });

        mapStream.print();
    }

    // 将多个流合并
    public static void transform_union(StreamExecutionEnvironment env) {
        DataStreamSource<String> source1 = env.socketTextStream("bigdata01", 9998);
        DataStreamSource<String> source2 = env.socketTextStream("bigdata01", 9999);

        // source1和source2的类型必须一样，多流合并，直接将流合在一起，之后操作都是在合并的这条流上
        DataStream<String> unionStream = source1.union(source2);
        // source1.union(source1).print(); // 输出两次

        unionStream.map(p -> p).disableChaining(); // 在合并的那条流上操作

        unionStream.print();
    }

    // KeyedStream → DataStream
    public static void transform_reduce(StreamExecutionEnvironment env) {
        // source
        env
                .socketTextStream("bigdata01", 9999)

                // transformation
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        for (String s : line.split(" ")) {
                            collector.collect(s);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s, 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> t) throws Exception {
                        return t.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                })

                // sink
                .print();
    }

    public static void transform_keyBy(StreamExecutionEnvironment env) {
        // source
        env
                .readTextFile("./data/access.log")

                // transformation
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Long time = Long.parseLong(splits[0].trim());
                        String domain = splits[1].trim();
                        Double traffic = Double.parseDouble(splits[2].trim());
                        return new Access(time, domain, traffic);
                    }
                })
                // 数字下标应用于元组类型
                /*.keyBy("domain")
                .sum("traffic")
                .print();*/

                /*.keyBy(1)
                .sum("traffic")
                .print();*/

                // 按照domain分组，按照traffic求和
                .keyBy(new KeySelector<Access, String>() {
                    @Override
                    public String getKey(Access access) throws Exception {
                        return access.getDomain();
                    }
                })
                .sum("traffic")

                // sink
                .print();
    }

    public static void transform_flatMap(StreamExecutionEnvironment env) {
        // source
        DataStreamSource<String> source = env.socketTextStream("bigdata01", 9999);

        // transformation
        source
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        for (String s : line.split(" ")) {
                            collector.collect(s);
                        }
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return !"spark".equals(s);
                    }
                })

                // sink
                .print();
    }

    public static void transform_filter(StreamExecutionEnvironment env) {
        // source
        DataStreamSource<String> source = env.readTextFile("./data/access.log");

        // transform
        SingleOutputStreamOperator<Access> filterStream =
                source
                        .map(new MapFunction<String, Access>() {
                            @Override
                            public Access map(String s) throws Exception {
                                String[] splits = s.split(",");
                                Long time = Long.parseLong(splits[0].trim());
                                String domain = splits[1].trim();
                                Double traffic = Double.parseDouble(splits[2].trim());
                                return new Access(time, domain, traffic);
                            }
                        })
                        .filter(new FilterFunction<Access>() {
                            @Override
                            public boolean filter(Access access) throws Exception {
                                return access.getTraffic() > 4000;
                            }
                        });

        // sink
        filterStream.print();
    }

    // rich的类，具有生命周期的方法（open，close，获取上下文的），继承自AbstractRichFunction
    // open方法：Initialization method for the function，调用该Function用几个并行度，就调用open方法几次
    // 设置并行度，实质上是对调用open方法的限制
    public static void transform_richMap(StreamExecutionEnvironment env) {
        class PKMapFunction extends RichMapFunction<String, Access> {

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            // 初始化操作，每个并行度都要执行
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("~~~~~~~open~~~~~~~");
            }

            @Override
            public void close() throws Exception {
                // System.out.println("~~~~~~~close~~~~~~~");
                super.close();
            }

            // 每条数据执行一次，跟并行度无关
            @Override
            public Access map(String value) throws Exception {
                System.out.println("~~~~~~~map~~~~~~~");
                String[] splits = value.split(",");
                long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                double traffic = Double.parseDouble(splits[2].trim());
                return new Access(time, domain, traffic);
            }
        }
        // env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("./data/access.log");
        SingleOutputStreamOperator<Access> mapStream = source.map(new PKMapFunction()).setParallelism(2);
        mapStream.print();
    }

    public static void transform_map(StreamExecutionEnvironment env) {

        // env.setParallelism(1);

        // source
        DataStreamSource<String> source = env.readTextFile("./data/access.log");

        // transform
        SingleOutputStreamOperator<Access> mapStream =
                source
                        .map(new MapFunction<String, Access>() {
                            @Override
                            public Access map(String s) throws Exception {
                                String[] splits = s.split(",");
                                Long time = Long.parseLong(splits[0].trim());
                                String domain = splits[1].trim();
                                Double traffic = Double.parseDouble(splits[2].trim());
                                return new Access(time, domain, traffic);
                            }
                        });

        // sink
        mapStream.print();
    }
}
