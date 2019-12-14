package com.cmcc.dmp.Utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * requirement   Hbase工具类
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/7 17:33
 */
public class HBaseUtils {
    private static final org.apache.log4j.Logger logger = Logger.getLogger(HBaseUtils.class);
    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static Connection connection;

    static {
        //1.获取配置连接对象
        Configuration configuration = HBaseConfiguration.create();
        // 2、设置hbase连接参数
        configuration.set(CONNECT_KEY,CONNECT_VALUE);
        // 3、获取connection的对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("连接HBase异常",e);
        }
    }

    /**
     * 获取admin对象
     * @return
     */
    public static Admin getAdmin(){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("获取admin时异常",e);
        }
        return admin;
    }

    /**
     * 关闭admin对象
     * @param admin
     */
    public static void closeAdmin(Admin admin){
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.warn("关闭admin时异常",e);
            }
        }
    }

    // 创建表
    public static void createTable(String tableName,String columnDescriptorName){
        Admin admin = HBaseUtils.getAdmin();
        System.out.println("创建表！！");

        try {
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            if (exists){
                System.out.println("表已经存在！不再创建");
            }else{
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnDescriptorName);
                columnDescriptor.setVersions(1,5);
                columnDescriptor.setTimeToLive(24*60*60);
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            HBaseUtils.closeAdmin(admin);
        }
    }

    // 追加 列族
    public static void addColumnDescriptor(String tableName,String columnDescriptorName) throws IOException {
        System.out.println("追加列族！");
        Admin admin = HBaseUtils.getAdmin();
        List<String> list=new ArrayList<>();
        //1.创建表描述器
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnDescriptor : columnFamilies){
            String nameAsString = columnDescriptor.getNameAsString();
            list.add(nameAsString);
        }
        if (list.contains(columnDescriptorName)){
            System.out.println("列族已经存在，不再创建");
        }else{
        //2.创建列族表述
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnDescriptorName);
        //3.设置列族版本从1到5
        columnDescriptor.setVersions(1,5);
        columnDescriptor.setTimeToLive(24*60*60);
        //4.将列族提娜佳到表中
        tableDescriptor.addFamily(columnDescriptor);
        //5.提交
        admin.modifyTable(TableName.valueOf(tableName),tableDescriptor);
        }


    }

    // 获取table对象
    public static Table getTable(String tableName){
        Table table = null;
        if (StringUtils.isNotEmpty(tableName)){
            try {
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
                logger.warn("获取表对象异常",e);
            }
        }
        return table;
    }

    // 关闭表对象
    public static void closeTable(Table table){
        if (table!=null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.warn("关闭table时异常",e);
            }
        }
    }

    // 插入数据
    public static void putData(Table table,String columnDescriptorName,String rowkey,String column,String info){
        System.out.println("导入数据");
        //Table table = HBaseUtils.getTable(tableName);
        try {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnDescriptorName), Bytes.toBytes(column), Bytes.toBytes(info));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("插入数据异常",e);
        }finally {
            HBaseUtils.closeTable(table);
        }
    }

    // 获取hbase的数据
    public static List<String> GetData(Table table, String columnDescriptorName, String column){
        List<String> list = new ArrayList<>();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(columnDescriptorName),Bytes.toBytes(column));
        // 获取扫描器
        try {
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()){
                Result result = iterator.next();
                String res = HBaseUtils.showResult(result);
                list.add(res);
                //System.out.println(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("获取扫描器异常",e);
        }
       return  list;
    }

    // 获取 hbase 数据
    public static String showResult(Result result){
        String res = "";
        CellScanner cellScanner = result.cellScanner();
        //System.out.print("rowKey: " + Bytes.toString(result.getRow()));
        res = Bytes.toString(result.getRow());
        try {
            while (cellScanner.advance()){
                Cell current = cellScanner.current();
//                System.out.print("\t" + new
//                        String(CellUtil.cloneFamily(current),"utf-8"));
//                System.out.print(" : " + new
//                        String(CellUtil.cloneQualifier(current),"utf-8"));
//                System.out.print("\t" + new
//                        String(CellUtil.cloneValue(current),"utf-8"));
//                System.out.println();
                res = res + new String(CellUtil.cloneValue(current));
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("判断是否有下一个单元格失败！",e);
        } catch (IOException e) {
            logger.error("克隆数据失败！",e);
        }
        return res;
    }

    public static void main(String[] args) throws IOException {
        //Table table = HBaseUtils.getTable("dmp_label_test");
        //List<String> tags = HBaseUtils.GetData(table, "tags", "20191211");
        //System.out.println(tags);
        HBaseUtils.addColumnDescriptor("cmcc_zsl:cmcc_kylin_metadata","aa");
    }
}
