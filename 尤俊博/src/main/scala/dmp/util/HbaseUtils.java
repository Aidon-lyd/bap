package dmp.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class HbaseUtils {
    private static Configuration configuration = null;
    private static HBaseAdmin hBaseAdmin = null;
    private static Admin admin = null;
    private static Connection conn = null;
    private static Table table;
    static {
        //1、获取配置连接对象
        configuration = new Configuration();
        //2、设置连接hbase的参数
        configuration.set("hbase.zookeeper.quorum", "node245:2181,node246:2181,node247:2181");
        //3、获取Admin对象
        try{
            conn = ConnectionFactory.createConnection(configuration);
            hBaseAdmin = (HBaseAdmin)conn.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 获取HbaseAdmin对象
     * @return
     */
    public static HBaseAdmin gethBaseAdmin(){
        return hBaseAdmin;
    }

    /**
     * 获取表
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(TableName tableName) throws IOException {
        return conn.getTable(tableName);
    }

    /**
     * 关闭连接
     * @param admin
     */
    public static void close(HBaseAdmin admin){
        try {
            if(admin!=null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void close(HBaseAdmin admin,Table table){
        try {
            if(admin!=null) {
                admin.close();
            }
            if(table!=null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(Table table){
        try {
            if(table!=null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 将数据插入表中
     * 创建表的语句：
     * create "userFlagInfo","baseInfo"
     * @param tablename  表的名称
     * @param familyName 表的列簇
     * @param dataMap    表的数据
     * @throws Exception
     */
/*    public static void put(String tablename,String rowkey,String familyName,
    Map<String,String> dataMap)throws Exception{
        //获取表
        Table table = getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        if(dataMap != null){
            for(Map.Entry<String,String> entry : dataMap.entrySet()){
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(key),Bytes.toBytes(value+""));
            }
        }
        table.put(put);
        close(table);
    }*/

    public static void put(String tablename,String familyName,String column,
                           Map<String,String> dataMap)throws Exception{
        //获取表
        Table table = getTable(TableName.valueOf(tablename));
        List<Put> list = new ArrayList<Put>();
        if(dataMap != null){
            for(Map.Entry<String,String> entry : dataMap.entrySet()){
                String key = entry.getKey();
                Put put = new Put(Bytes.toBytes(key));
                Object value = entry.getValue();
                //System.out.println(value);
                put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(column),Bytes.toBytes(value+""));
                list.add(put);
            }
        }
        table.put(list);

        close(table);
    }

    public static void put(String tablename,String rowkey,String familyName,
                           String colum, String data)throws Exception{
        //获取表
        Table table = getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(colum),Bytes.toBytes(data));
        table.put(put);
        close(table);
    }

    public String getData(String tableName,String rowKey,String familyName, String clounName) throws IOException {
        Table table = getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] resultBytes = result.getValue(familyName.getBytes(),clounName.getBytes());
        if(resultBytes == null){
            return null;
        }
        return new String(resultBytes);
    }
}
