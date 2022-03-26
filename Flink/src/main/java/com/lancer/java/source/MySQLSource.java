package com.lancer.java.source;

import com.lancer.java.utils.MySQLUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLSource {

    public static class StudentSource extends RichSourceFunction<Student> {

        Connection connection;
        PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("~~~~~~open~~~~~~"); // 调用一次
            connection = MySQLUtils.getConnection();
            preparedStatement = connection.prepareStatement("select * from student");
        }

        @Override
        public void close() throws Exception {
            MySQLUtils.close(preparedStatement);
            MySQLUtils.close(connection);
        }

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                ctx.collect(new Student(id, name, age));
            }
        }

        @Override
        public void cancel() {
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    @ToString
    public static class Student {
        private int id;
        private String name;
        private int age;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> source = env.addSource(new StudentSource());
        System.out.println(source.getParallelism());
        source.print();

        env.execute("MySQLSource");
    }
}
