package com.lancer.java.sink;

import com.lancer.java.transformations.TransformationApp;
import com.lancer.java.utils.MySQLUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MySQLSink {

    public static class AccessSink extends RichSinkFunction<Tuple2<String, Double>> {

        Connection connection;
        PreparedStatement insertPreparedStatement;
        PreparedStatement updatePreparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = MySQLUtils.getConnection();
            insertPreparedStatement = connection.prepareStatement("insert into traffic(domain, traffic) values(?, ?)");
            updatePreparedStatement = connection.prepareStatement("update traffic set traffic = ? where domain = ?");
        }

        @Override
        public void close() throws Exception {
            MySQLUtils.close(updatePreparedStatement);
            MySQLUtils.close(insertPreparedStatement);
            MySQLUtils.close(connection);
        }

        @Override
        public void invoke(Tuple2<String, Double> value, Context context) throws Exception {
            System.out.println("==========invoke==========" + value.f0 + "-->" + value.f1);
            updatePreparedStatement.setDouble(1, value.f1);
            updatePreparedStatement.setString(2, value.f0);
            updatePreparedStatement.execute();
            if (updatePreparedStatement.getUpdateCount() == 0) {
                insertPreparedStatement.setString(1, value.f0);
                insertPreparedStatement.setDouble(2, value.f1);
                insertPreparedStatement.execute();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("./data/access.log");

        SingleOutputStreamOperator<TransformationApp.Access> mapStream =
                source
                        .map(new MapFunction<String, TransformationApp.Access>() {
                            @Override
                            public TransformationApp.Access map(String value) throws Exception {
                                String[] splits = value.split(",");
                                Long time = Long.parseLong(splits[0].trim());
                                String domain = splits[1].trim();
                                Double traffic = Double.parseDouble(splits[2].trim());
                                return new TransformationApp.Access(time, domain, traffic);
                            }
                        });

        SingleOutputStreamOperator<Tuple2<String, Double>> result =
                mapStream
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

        result.print();
        result.addSink(new AccessSink());

        env.execute("MySQLSink");
    }
}
