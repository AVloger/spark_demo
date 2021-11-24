package com.ClickProject;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHelper {

    public static final String url ="jdbc:mysql://192.168.182.201:3306/userdb";
    public static final String name="com.mysql.jdbc.Driver";
    public static final String user="sqoop";
    public static final String password="sqoop";

    //获取数据库连接
    public Connection conn=null;

    public DBHelper(){
        try {
            Class.forName(name);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }


    public void close(){
        try {
            this.conn.close();
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

}