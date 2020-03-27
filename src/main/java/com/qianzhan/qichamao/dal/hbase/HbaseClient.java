package com.qianzhan.qichamao.dal.hbase;

import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HbaseClient {
    @Getter
    private static Connection connection;
    @Getter
    private static Admin admin;
    static {
        Configuration config = HBaseConfiguration.create();
        try {
            config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
            // initialize by hard coding
//            config.set("hbase.zookeeper.quorum", "127.0.0.1");
//            config.set("hbase.rootdir", "file:///home/jian/sha/tool/hbase-2.2.3");
//            config.set("hbase.zookeeper.property.dataDir", "/home/jian/sha/tool/hbase-2.2.3/zookeeper");
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            admin = null;
        }
        if (connection != null) {
            try {
                connection.close();
                ;
            } catch (IOException e) {
                e.printStackTrace();
            }
            connection = null;
        }
    }

    public static Connection connect() throws IOException {
        return ConnectionFactory.createConnection();
    }

    public static void updateTable(String table) throws Exception {
        Connection connection = connect();
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(table);
        admin.disableTable(tn);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);

        TableDescriptor td = builder.build();
        ColumnFamilyDescriptor cfd = td.getColumnFamilies()[0];
        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(cfd.getName());
        cfdb.setCompactionCompressionType(Compression.Algorithm.GZ);
        cfdb.setMaxVersions(HConstants.ALL_VERSIONS);
        builder.modifyColumnFamily(cfdb.build());
        admin.modifyTable(builder.build());
        admin.enableTable(tn);
        admin.close();
        connection.close();
    }



    public static void put(String table, String key, String family, String column, long ts, String value) throws Exception{
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        Put put = new Put(Bytes.toBytes(key));
        if (ts <= 0) {
            ts = new Date().getTime();
//            ts = new SimpleDateFormat("yy/MM/dd HH:mm:ss").parse("2020-02-27 19:19:19").getTime();
        }
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), ts, Bytes.toBytes(value));
        t.put(put);
        t.close();
        connection.close();
    }

    public static void append(String table, String key, String family, String column, String value) throws Exception {
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        Append append = new Append(Bytes.toBytes(key));
        append.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        t.append(append);
        t.close();
        connection.close();
    }

    public static void incr(String table, String key, String family, String column, long value) throws Exception {
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        Increment increment = new Increment(Bytes.toBytes(key));
        increment.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), value);
        t.increment(increment);
        t.close();
        connection.close();
    }

    /**
     * get a cell value
     * @param table
     * @param key
     * @param family
     * @param column
     * @throws Exception
     */
    public static void get(String table, String key, String family, String column) throws Exception {
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(key));
        get.addFamily(Bytes.toBytes(family));
        Result result = t.get(get);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
//        Bytes.toLong(value);
//        Bytes.toBoolean(value);
//        Bytes.toString(value);
        t.close();
        connection.close();
    }

    public static void get(String table, String[] keys, String family, String column) throws Exception {
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        List<Get> gets = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; ++i) {
            Get get = new Get(Bytes.toBytes(keys[i]));
            if (!MiscellanyUtil.isBlank(family))
                get.addFamily(Bytes.toBytes(family));

            gets.add(get);
        }
        Result[] rs = t.get(gets);
        String[] vs = new String[rs.length];
        for (int i = 0; i < rs.length; ++i) {
            vs[i] = Bytes.toString(rs[i].getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
        }
        t.close();
        connection.close();
    }

    public static void scan(String table, String[] keys, String family, String column) throws Exception {
        Connection connection = connect();
        Table t = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();

        List<Filter> filters = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; ++i) {
            filters.add(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(keys[i]))));
        }
        scan.addFamily(Bytes.toBytes(family)).setBatch(keys.length).setFilter(
                new FilterList(FilterList.Operator.MUST_PASS_ONE,filters));
        List<String> vs = new ArrayList<String>();
        ResultScanner rs = t.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            String v = Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
            vs.add(v);
            String rowKey = Bytes.toString(r.getRow());
            System.out.println(String.format("(%s, %s)", rowKey, v));
        }
        t.close();
        connection.close();
    }

}
