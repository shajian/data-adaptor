package com.qianzhan.qichamao.dal;

import com.qianzhan.qichamao.util.DbConfigBus;

import java.sql.*;
import java.util.List;

public class JDBCMSSQL {
    private static Connection conn = null;

    static {
        try {
            Class.forName(DbConfigBus.getDbConfig_s("MSSQL_DRIVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
            conn = DriverManager.getConnection(
                    DbConfigBus.getDbConfig_s("MSSQL_COM_URL", null),
                    DbConfigBus.getDbConfig_s("MSSQL_COM_USER", null),
                    DbConfigBus.getDbConfig_s("MSSQL_COM_PASS", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void query(String sql) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

//            if sql keeps unchanging, `ps` can be assigned once and reuse after
//                    to make efficiency high
//            PreparedStatement ps = conn.prepareStatement(sql);
//            ResultSet rs = ps.executeQuery();

//            actually, however, sql is often a template with an argument
//            for example:
//            String sql = "select top 100 * from Users where id > ?";
//            PreparedStatement ps = conn.prepareStatement(sql);
//            ps.setInt(1, 100);  // select users whose id > 100
//            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
//                column index starts from 1
                String name = rs.getString(2);
                String pos = rs.getString(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param sql e.g. update Users set name=?, sex=?, age=? where id=?
     * @param strs
     */
    public static void update(String sql, List<String> strs) {
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            int i = 0;
            for (String str: strs
                 ) {
                i += 1;
                ps.setString(i, str);
            }
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param sql delete from Users where id=?
     * @param id
     */
    public static void delete(String sql, String id) {
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert(String sql, List<String> strs) {
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            int i = 0;
            for (String str :
                    strs) {
                ps.setString(i, str);
                i += 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
