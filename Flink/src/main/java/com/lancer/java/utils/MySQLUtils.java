package com.lancer.java.utils;

import java.sql.*;

/**
 * 有问题，关闭连接后，其他连接就断开了
 */
public class MySQLUtils {

    private static Connection connection;

    private MySQLUtils(){
    }

    public static synchronized Connection getConnection() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection("jdbc:mysql://bigdata05:3306/flinkTest?useSSL=false", "root", "123456");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static synchronized void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static synchronized void close(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
