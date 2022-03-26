package com.lancer.java;

import com.lancer.java.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

// 从kafka中读数据
public class Test {

    @org.junit.Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);


        source
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        if (value != 3) out.collect(value);
                    }
                })
                .print();

        env.execute("test");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (args == null | args.length != 4) {
            System.err.println("请输入参数！ --producerTopic 主题名，--consumerTopic 主题名");
            System.exit(-1);
        }

        ParameterTool tool = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(tool);

        Properties p = new Properties();

        p.load(Test.class.getClassLoader().getResourceAsStream("consumer.properties"));

        OutputTag<String> outputTag = new OutputTag<String>("adult") {
        };

        // topic ==> flinkTopic
        // DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>(tool.get("consumerTopic"), new SimpleStringSchema(), p));
        DataStreamSource<String> source = env.socketTextStream("bigdata01", 9999);
        SingleOutputStreamOperator<Person> outputDataStream = source
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (!"".equals(value)) {
                            String[] splits = value.split(",");
                            return splits.length == 3 && (Integer.parseInt(splits[2]) >= 0 && Integer.parseInt(splits[2]) <= 100);
                        }
                        return false;
                    }
                })
                // 判断是否成年，成年流直接打印，未成年流放入kafka
                .process(new ProcessFunction<String, Person>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Person> out) throws Exception {
                        String[] splits = value.split(",");
                        String name = splits[0].trim();
                        String gender = splits[1].trim();
                        int age = Integer.parseInt(splits[2].trim());
                        Person person = new Person(name, gender, age);
                        if (age >= 18) {
                            out.collect(person);
                        } else {
                            ctx.output(outputTag, value);
                        }
                    }
                });
        outputDataStream.print();

        outputDataStream
                .getSideOutput(outputTag)
                .flatMap(new RichFlatMapFunction<String, String>() {

                    private KafkaProducer<String, String> producer;
                    private String topic;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ParameterTool tool = (ParameterTool) this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        producer = new KafkaProducer<>(p);
                        topic = tool.get("producerTopic");
                    }

                    @Override
                    public void close() throws Exception {
                        if (producer != null) {
                            producer.close();
                        }
                    }

                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {

                        // topic => flinkTest
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
                        producer.send(record);
                    }
                });

        env.execute(Test.class.getSimpleName());
    }
}

