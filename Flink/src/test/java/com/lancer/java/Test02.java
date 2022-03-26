package com.lancer.java;

import com.lancer.java.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Test02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();

        p.load(Test.class.getClassLoader().getResourceAsStream("producer.properties"));

        OutputTag<String> outputTag = new OutputTag<String>("adult") {
        };

        // DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("flinkTopic", new SimpleStringSchema(), p));
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
                .addSink(new FlinkKafkaProducer<>("flinkTest", new SimpleStringSchema(), p));

        env.execute(Test.class.getSimpleName());
    }
}
