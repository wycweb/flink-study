package com.wangyichao.bigdata.utils;

import java.sql.*;
import java.util.ResourceBundle;

/**
 * MySQL JDBC链接基础类
 */
public class MysqlJDBCUtil {

    private static String url;
    private static String user;
    private static String password;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("mysql-env");

        url = bundle.getString("url");
        user = bundle.getString("username");
        password = bundle.getString("password");
    }

    /**
     * 获取连接方法
     *
     * @return
     */
    public static Connection getConnection() {

        Connection conn = null;
        try {

            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 释放资源方法
     *
     * @param conn
     */
    public static void closeConnection(Connection conn) {

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放资源方法
     *
     * @param conn
     */
    public static void closeConnection(Connection conn, PreparedStatement pstmt) {


        try {

            if (pstmt == null || conn == null) {
                conn.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行增改查操作
     *
     * @param sqlString
     * @return
     */
    public static ResultSet executeQuery(Connection conn, String sqlString) {
        ResultSet resultSet = null;
        PreparedStatement pstmt;

        try {
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sqlString);
            resultSet = pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }
}
