package com.lancer.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @Author lancer
 * @Date 2022/2/18 2:44 下午
 * @Description
 */
public class Main {
    private static Connection conn;

    @Before
    public void getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03,bigdata04,bigdata05");
        conn = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testGetConnection() {
        System.out.println(conn);
    }

    /* DDL 操作需要Admin对象*/

    /**
     * 创建namespace --> 命令：create_namespace
     */
    @Test
    public void testCreateNamespace() throws IOException {
        Admin admin = conn.getAdmin();
        // 设置自动负载均衡
        admin.balance(true);
        // 创建namespace构建器
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("ns");
        // 设置属性
        builder.addConfiguration("author", "lancer");
        // 构建namespace描述器
        NamespaceDescriptor namespaceDescriptor = builder.build();
        // 创建namespace
        admin.createNamespace(namespaceDescriptor);
        // 关闭admin对象
        admin.close();
    }

    /**
     * 获取namespace --> 命令：list_namespace
     */
    @Test
    public void testGetAllNamespace() throws IOException {
        Admin admin = conn.getAdmin();
        // 获取单个namespaceDescriptor
        admin.getNamespaceDescriptor("ns");

        // 获取所有namespace
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            String name = namespaceDescriptor.getName();
            // 如果namespace为ns，则输出其所有者
            if ("ns".equals(name)) {
                String author = namespaceDescriptor.getConfigurationValue("author");
                System.out.print(author + " ============= ");
            }
            System.out.println(name);
        }
        admin.close();
    }

    /**
     * 删除namespace --> 命令：drop_namespace
     */
    @Test
    public void testDelNamespace() throws IOException {
        Admin admin = conn.getAdmin();

        // 删除namespace,namespace必须为空
        admin.deleteNamespace("ns");

        admin.close();
    }

    /**
     * 创建table --> 命令：create 'ns:student', 'info'
     */
    @Test
    public void testCreateTable() throws IOException {
        Admin admin = conn.getAdmin();
        // 指定namespace和表名
        TableName tableName = TableName.valueOf("ns", "student");
        // TableName tableName1 = TableName.valueOf("ns:student");

        // 判断表是否存在
        if (!admin.tableExists(tableName)) {
            // 老式api
            /*HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);*/

            // 新式api
            // 通过构建者模式构建tableDescriptor，可以设置属性
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            // 构建columnDescriptor
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder("info1".getBytes(StandardCharsets.UTF_8));
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder2 = ColumnFamilyDescriptorBuilder.newBuilder("info2".getBytes(StandardCharsets.UTF_8));
            // 中间可以设置列簇属性
            ColumnFamilyDescriptor columnFamilyDescriptor1 = columnFamilyDescriptorBuilder1.build();
            ColumnFamilyDescriptor columnFamilyDescriptor2 = columnFamilyDescriptorBuilder2.build();
            // 将列簇封装在一起
            ArrayList<ColumnFamilyDescriptor> list = new ArrayList<>();
            list.add(columnFamilyDescriptor1);
            list.add(columnFamilyDescriptor2);
            // 将columnDescriptor添加到tableDescriptor中
            tableDescriptorBuilder.setColumnFamilies(list);
            // 获取tableDescriptor
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            // 创建表
            admin.createTable(tableDescriptor);
            admin.flush(tableName);
        } else {
            System.out.println("table already exists!");
        }
        admin.close();
    }

    /**
     * 创建预region的表
     * 1. 在表初次插入数据的时候，所有的数据会被插入到一个region中，只有单个RegionServer服务，可以采用预region解决插入的热点问题
     * 2. 在查询的时候，你查询的数据恰好都在一个region中，可以采用预region解决查询的热点问题
     */
    @Test
    public void testCreateTableWithPrepareRegions() throws IOException {
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns:pre_student");

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes(StandardCharsets.UTF_8)).build();

        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        // 切割点，3个切割点分成4个region
        byte[][] splitKeys = {
                "e".getBytes(StandardCharsets.UTF_8),
                "h".getBytes(StandardCharsets.UTF_8),
                "n".getBytes(StandardCharsets.UTF_8)
        };

        admin.createTable(tableDescriptor, splitKeys);
        admin.close();
    }

    /**
     * 删除表 --> 命令：(disable 'ns:student' --> drop 'ns:student')
     */
    @Test
    public void testDelTable() throws IOException {
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns:student");

        if (admin.tableExists(tableName)) {
            // first disable表
            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
        } else {
            System.out.println("table not exists!");
        }

        admin.close();
    }

    /**
     * 获取所有表信息
     */
    @Test
    public void testDescribeTable() throws IOException {
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns:student");

        // 获取指定表的信息 --> 命令： describe 'ns:student'
        TableDescriptor ontTableDescriptor = admin.getDescriptor(tableName);
        // 通过tableDescriptor获取到所有的columnFamilyDescriptor
        ColumnFamilyDescriptor[] columnFamilyDescriptors = ontTableDescriptor.getColumnFamilies();
        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            // 获取每个列簇的名称
            System.out.println(columnFamilyDescriptor.getNameAsString());
            System.out.println("maxVersion: " + columnFamilyDescriptor.getMaxVersions());
            System.out.println("ttl: " + columnFamilyDescriptor.getTimeToLive());

            // 获取单个列簇的所有属性
            Map<String, String> configuration = columnFamilyDescriptor.getConfiguration();

            // 通过名称获取单个列簇的属性
            byte[] value = columnFamilyDescriptor.getValue("TTL".getBytes(StandardCharsets.UTF_8));
            String ttl = columnFamilyDescriptor.getConfigurationValue("TTL");
        }

        // 获取所有表的信息，例如（名称 --> 命令：list）
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for (TableDescriptor tableDescriptor : tableDescriptors) {
            String ns = tableDescriptor.getTableName().getNamespaceAsString();
            String name = tableDescriptor.getTableName().getNameAsString();
            System.out.println(ns + " ------ " + name);
        }

        admin.close();
    }

    /**
     * 添加列簇
     */
    @Test
    public void testAddColumnFamily() throws IOException {
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns", "student");

        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("info_test".getBytes(StandardCharsets.UTF_8))
                .setMaxVersions(2)
                .build();

        admin.addColumnFamily(tableName, columnFamilyDescriptor);

        admin.close();
    }

    /**
     * 删除列簇，也可通过Table对象删除
     */
    @Test
    public void testDelColumnFamily() throws IOException {
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns:student");

        admin.deleteColumnFamily(tableName, "info_test".getBytes(StandardCharsets.UTF_8));

        admin.close();
    }


    /* DML 需要使用Table对象 */

    /**
     * 增
     * <p>
     * 1. put添加每一条数据，需要RPC网络请求，效率不高
     * 2. 缓存批次插入数据
     * 3. bulkload批量导入数据： 普通的数据文件  --> MR --> HFile
     * 3。 HBase表中管理的数据，是存储在HDFS傻姑娘的特殊文件HFile
     */
    // 1. 一次增一条
    @Test
    public void testAddOneRowData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("ns:student"));

        // 获取行，指定rowKey
        Put put1 = new Put("rk001".getBytes(StandardCharsets.UTF_8));

        // 指定某个列簇下，存哪些列和值
        put1.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8),
                "zhangsan".getBytes(StandardCharsets.UTF_8));

        Put put2 = new Put("rk002".getBytes(StandardCharsets.UTF_8));
        put2.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "age".getBytes(StandardCharsets.UTF_8),
                "23".getBytes(StandardCharsets.UTF_8));

        ArrayList<Put> list = new ArrayList<>();
        list.add(put1);
        list.add(put2);

        // 将数据插入表中
        // table.put(put1);
        table.put(list);
        table.close();
    }

    // 通过缓存的方式添加数据
    @Test
    public void testAddRowDataByBuffer() throws IOException {
        // 表示一个可以缓存数据的表对象，缓存到一定大小后，再写入HBase
        BufferedMutator bufferedMutator = conn.getBufferedMutator(TableName.valueOf("ns:student"));

        Put put1 = new Put("rk003".getBytes(StandardCharsets.UTF_8));
        put1.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8),
                "lisi".getBytes(StandardCharsets.UTF_8));
        put1.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "age".getBytes(StandardCharsets.UTF_8),
                "11".getBytes(StandardCharsets.UTF_8));

        Put put2 = new Put("rk004".getBytes(StandardCharsets.UTF_8));
        put2.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8),
                "wangwu".getBytes(StandardCharsets.UTF_8));
        put2.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "age".getBytes(StandardCharsets.UTF_8),
                "15".getBytes(StandardCharsets.UTF_8));

        ArrayList<Put> list = new ArrayList<>();
        list.add(put1);
        list.add(put2);

        bufferedMutator.mutate(list);

        long size = bufferedMutator.getWriteBufferSize();
        System.out.println(size);

        bufferedMutator.flush();
        bufferedMutator.close();
    }

    /**
     * 删
     */
    // 删除某条数据
    @Test
    public void testDelRows() throws IOException {
        Table table = conn.getTable(TableName.valueOf("ns:student"));

        // 通过指定rowKey删除数据，不会删除列簇
        Delete delete1 = new Delete("rk003".getBytes(StandardCharsets.UTF_8));
        Delete delete2 = new Delete("rk004".getBytes(StandardCharsets.UTF_8));

        ArrayList<Delete> list = new ArrayList<>();
        list.add(delete1);
        list.add(delete2);

        // 删除多行数据
        table.delete(list);
        table.close();
    }

    // 删除列簇
    @Test
    public void testColumnFamily() throws IOException {
        Table table = conn.getTable(TableName.valueOf("ns:student"));

        Delete delete = new Delete("rk002".getBytes(StandardCharsets.UTF_8));

        delete.addFamily("info".getBytes(StandardCharsets.UTF_8));

        // 指定删除的属性，单元格
        // delete.addColumn("info".getBytes(StandardCharsets.UTF_8), "name".getBytes(StandardCharsets.UTF_8));

        table.delete(delete);

        table.close();
    }

    /**
     * 查
     */
    // Get
    @Test
    public void testGet() throws IOException {
        Table table = conn.getTable(TableName.valueOf("ns:student"));

        Get get = new Get("rk001".getBytes(StandardCharsets.UTF_8));
        // 设置属性
        /*get.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8));*/
        Result result = table.get(get);
        // 通过列簇和属性获取值
        new String(
                result.getValue("info".getBytes(StandardCharsets.UTF_8),
                        "name".getBytes(StandardCharsets.UTF_8))
        );

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            // 使用CellUtil工具类获取信息
            // 获取rowKey
            byte[] rowKey = CellUtil.cloneRow(cell);
            // 获取列簇
            byte[] family = CellUtil.cloneFamily(cell);
            // 获取属性名
            byte[] column = CellUtil.cloneQualifier(cell);
            // 获取值
            byte[] value = CellUtil.cloneValue(cell);

            System.out.println("========================");
            System.out.println("rowKey: " + Bytes.toString(rowKey));
            System.out.println("family: " + Bytes.toString(family));
            System.out.println("column: " + Bytes.toString(column));
            System.out.println("value: " + Bytes.toString(value));
        }
        table.close();
    }

    // Scan
    @Test
    public void testScan() throws IOException {
        Table table = conn.getTable(TableName.valueOf("ns:student"));

        Scan scan = new Scan();

        scan.withStartRow("rk001".getBytes(StandardCharsets.UTF_8));
        scan.withStopRow("rk002".getBytes(StandardCharsets.UTF_8), true);
        // 设置显示条数
        scan.setLimit(30);
        scan.addFamily("info".getBytes(StandardCharsets.UTF_8));

        ResultScanner sc = table.getScanner(scan);
        for (Result result : sc) {
            // 获取所有单元格
            while (result.advance()) {
                Cell cell = result.current();
                // 获取rowKey
                byte[] rowKey = CellUtil.cloneRow(cell);
                // 获取列簇
                byte[] family = CellUtil.cloneFamily(cell);
                // 获取属性名
                byte[] column = CellUtil.cloneQualifier(cell);
                // 获取值
                byte[] value = CellUtil.cloneValue(cell);
                // 获取时间戳
                long timestamp = cell.getTimestamp();
                System.out.println(new String(rowKey) + ":" + new String(family) + ":" + new String(column) + ":" + new String(value) + ":" + timestamp);
            }
        }

        table.close();
    }

    @After
    public void closeConnection() throws IOException {
        conn.close();
    }
}
