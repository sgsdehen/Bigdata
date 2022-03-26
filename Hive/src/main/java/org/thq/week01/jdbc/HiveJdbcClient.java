package org.thq.week01.jdbc;

import java.sql.*;

public class HiveJdbcClient {
    public static void main(String[] args) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection("jdbc:hive2://bigdata01:10000/default", "root", "");
            Statement statement = conn.createStatement();
            ResultSet databases = statement.executeQuery("show databases");
            while (databases.next()) {
                System.out.println(databases.getString(1));
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }


    }
}
