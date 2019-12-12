package com.cmcc.dmp.Utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * requirement
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
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnDescriptorName);
            columnDescriptor.setVersions(1,5);
            columnDescriptor.setTimeToLive(24*60*60);
            tableDescriptor.addFamily(columnDescriptor);

            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            HBaseUtils.closeAdmin(admin);
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

}
