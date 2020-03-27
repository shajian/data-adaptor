package com.qianzhan.qichamao.dal.hbase;

import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.util.Asserts;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HbaseRepository<T> {
    private Admin admin;
    private Connection connection;
    private Class<T> clazz;
    private String table_name;
    private String[] families;
    private int[] max_versions;

    public HbaseRepository() {
        connection = HbaseClient.getConnection();
        admin = HbaseClient.getAdmin();
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        clazz = (Class<T>) type.getActualTypeArguments()[0];
        HbaseTableMeta meta = clazz.getAnnotation(HbaseTableMeta.class);
        table_name = meta.table_name();
        families = meta.families();
        max_versions = meta.max_versions();
    }

    public String[] list() throws IOException {
        TableName[] tns = admin.listTableNames();
        String[] names = new String[tns.length];
        for (int i = 0; i < tns.length; ++i)
            names[i] = tns[i].getNameAsString();
        return names;
    }

    public void create() throws IOException {
        TableName tn = TableName.valueOf(table_name);
        if (admin.tableExists(tn)) {
            // todo log
            return;
        }

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (int i = 0; i < families.length; ++i) {
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(families[i]));
            cfdb.setMaxVersions(max_versions[i]);
            builder.setColumnFamily(cfdb.build());
        }
        admin.createTable(builder.build());
    }

    public void drop() throws IOException {
        TableName tn = TableName.valueOf(table_name);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
            return;
        }
        // todo log table is not existed
    }

    /**
     * add new families into an existed table
     * @param map family name -> max_version
     * @throws Exception
     */
    public void addFamilies(Map<String, Integer> map) throws Exception {
        Asserts.check(false, "Can not add families to an existed table." +
                "Some flexibility is lost for high efficiency");
        TableName tn = TableName.valueOf(table_name);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String family : map.keySet()) {
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
            cfdb.setMaxVersions(map.get(family));
            builder.setColumnFamily(cfdb.build());
        }
        admin.disableTable(tn);
        admin.modifyTable(builder.build());
        admin.enableTable(tn);
    }


    public void delete(String key) {
        delete(key, null, null, 0, false);
    }

    public void delete(String key, String family) {
        delete(key, family, null, 0, false);
    }

    public void delete(String key, String family, String column) {
        delete(key, family, column, 0, false);
    }

    /**
     * delete data according to row key, maybe other modifiers(column family/attribute)
     *  are also specified
     * @param key
     * @param family
     * @param column
     * @param version
     * @param equal true, if delete the specified version; false, if delete the less then or equal to version
     */
    public void delete(String key, String family, String column, long version, boolean equal) {
        Asserts.check(!MiscellanyUtil.isBlank(key), "row key mustn't be empty");
//        Asserts.check(MiscellanyUtil.isBlank(column) ||
//                !MiscellanyUtil.isBlank(family), "family must be given when column isn't empty");
        Delete delete = new Delete(Bytes.toBytes(key));

        if (!MiscellanyUtil.isBlank(family)) {
            if (!MiscellanyUtil.isBlank(column)) {
                if (version > 0) {
                    if (equal)
                        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), version);// del specified version
                    else
                        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column), version);
                }
                else
                    delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));    // delete all versions
            } else if (version > 0) {
                if (equal)
                    delete.addFamilyVersion(Bytes.toBytes(family), version);   // delete specified version
                else
                    delete.addFamily(Bytes.toBytes(family), version);   // delete all versions that <= specified version
            } else {
                delete.addFamily(Bytes.toBytes(family));
            }
        }

        try {
            Table table = connection.getTable(TableName.valueOf(table_name));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            // todo log
        }
    }

    public Result[] get(String[] keys, String[] families) throws IOException {
        Table table = connection.getTable(TableName.valueOf(table_name));
        List<Get> gets = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; ++i) {
            Get get = new Get(Bytes.toBytes(keys[i]));
            for (int j = 0; j < families.length; ++j)
                get.addFamily(Bytes.toBytes(families[i]));
            gets.add(get);
        }
        Result[] rs = table.get(gets);

        table.close();
        return rs;
    }

    public Result[] get(HbaseInput<T> input) throws IOException {
        Table table = connection.getTable(TableName.valueOf(table_name));
        String[] keys = input.getKeys();
        String[] families = input.getFamilies();
        Result[] rs = null;
        if (input.getGetMode() == HbaseInput.GetMode.get) {
            List<Get> gets = new ArrayList<>(keys.length);
            for (int i = 0; i < keys.length; ++i) {
                Get get = new Get(Bytes.toBytes(keys[i]));
                for (int j = 0; j < families.length; ++j)
                    get.addFamily(Bytes.toBytes(families[j]));
                gets.add(get);
            }
            rs = table.get(gets);
        } else if (input.getGetMode() == HbaseInput.GetMode.scan) {
            Scan scan = new Scan();

            List<Filter> filters = new ArrayList<>(keys.length);
            for (int i = 0; i < keys.length; ++i) {
                filters.add(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(keys[i]))));
            }
            for (int j = 0; j < families.length; ++j)
                scan.addFamily(Bytes.toBytes(families[j]));
            scan.setCaching(keys.length).setBatch(keys.length).setFilter(
                    new FilterList(FilterList.Operator.MUST_PASS_ONE,filters));
            ResultScanner scanner = table.getScanner(scan);
            rs = scanner.next(keys.length);
            scanner.close();
        }
        return rs;
    }
}
