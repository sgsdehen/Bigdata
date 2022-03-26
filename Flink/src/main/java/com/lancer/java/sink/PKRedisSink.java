package com.lancer.java.sink;

import com.lancer.java.transformations.TransformationApp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class PKRedisSink {

    // 单机模式的redis
    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Double>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            // additionalKey指定key名字
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Double> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Double> data) {
            return data.f1 + "";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("bigdata01").build();

        DataStreamSource<String> source = env.readTextFile("./data/access.log");

        SingleOutputStreamOperator<Tuple2<String, Double>> stream = source
                .map(new MapFunction<String, TransformationApp.Access>() {
                    @Override
                    public TransformationApp.Access map(String value) throws Exception {
                        String[] splits = value.split(",");
                        Long time = Long.parseLong(splits[0].trim());
                        String domain = splits[1].trim();
                        Double traffic = Double.parseDouble(splits[2].trim());
                        return new TransformationApp.Access(time, domain, traffic);
                    }
                })
                .keyBy(new KeySelector<TransformationApp.Access, String>() {
                    @Override
                    public String getKey(TransformationApp.Access value) throws Exception {
                        return value.getDomain();
                    }
                })
                .sum("traffic")
                .map(new MapFunction<TransformationApp.Access, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(TransformationApp.Access value) throws Exception {

                        return Tuple2.of(value.getDomain(), value.getTraffic());
                    }
                });

        stream.addSink(new RedisSink<Tuple2<String, Double>>(conf, new RedisExampleMapper()));

        env.execute("PKRedisSink");
    }
}
