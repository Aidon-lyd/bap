package com.jiangxi.util;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public final class HbaseUtil {
    /*
     * 获取连接方法
     */
    public static Connection getConnection(){
        //1、获取conf对象
        Configuration cfg=HBaseConfiguration.create();
        //2、指定hbase的zk集群地址
        cfg.set("hbase.zookeeper.quorum","node245,node246,node247");
        //3、指定hbase的zk集群的端口号
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        //4.获取连接
        Connection conn=null;
        try {
            conn=ConnectionFactory.createConnection(cfg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }
    /*
     * 关闭连接方法
     */
    public static void close(Connection conn){
        if(conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("关闭失败");
            }
        }
    }
}

