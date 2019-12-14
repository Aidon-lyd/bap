package com.cmcc.dmp.Utils;

import breeze.linalg.product;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cmcc.dmp.bean.TagsBean;
import jdk.nashorn.internal.codegen.ClassEmitter;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * requirement ES工具类
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/13 16:10
 */
public class ESUtils {

    private static TransportClient client;
    /**
     * 获取 TransportClient 对象

     * @return
     * @throws UnknownHostException
     */
    public static TransportClient getClient() throws UnknownHostException {
        //TransportClient client = null;
        //Settings settings = Settings.builder().put("cluster.name","my-application")
        Settings settings = Settings.builder().put("cluster.name","node245-es")
                .build();
        client = new PreBuiltTransportClient(settings);
//        Random random = new Random();
//        int i = random.nextInt(3);
//        if (i == 0){
//            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop02"),9300));
//        }else if (i == 1){
//            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop01"),9300));
//        }else{
//            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop03"),9300));
//        }
		System.setProperty("es.set.netty.runtime.available.processors", "false");
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("node245"), 9300));
        return client;
    }

    /**
     * 创建索引
     * @param IndexName
     */
    public static void createIndex(TransportClient client , String IndexName){
        client.admin().indices().prepareCreate(IndexName).get();

       // ESUtils.cleanUp(client);
    }

    // 在ES6.x 版本不允许一个index下有多个type！
    public static void putData(TransportClient client ,String json,String IndexName , String type){
        JSONObject jsonObject = JSON.parseObject(json);
        String rowkey = jsonObject.getString("rowkey");
        String label = jsonObject.getString("label");
        IndexResponse indexResponse =
                client.prepareIndex(IndexName,type,rowkey)
                .setSource(JSON.toJSONString(new TagsBean(rowkey,label)), XContentType.JSON).get();
        System.out.println("返回的结果是：" + indexResponse);
        //client.close();
    }

    public static void cleanUp(TransportClient client) {
        if (client != null) {
            client.close();
        }
    }


    public static void main(String[] args) throws UnknownHostException {
//        String key = "cluster.name";
//        //String value = "node245-es";
//        String value = "bigdata";
        String IndexName = "dmptest";
        String json = "{\"rowkey\":\"-1897127431\",\"label\":\"WIFI D00020001:1.8,K特别版:1.8,20d15c13462196ab41d8fa95dfaf894c:0.0,Android D00010001:1.8,K18以上:1.8,APP爱奇艺:1.8,LN视频前贴片:1.8,ZP广东省:1.8,CN100018:1.8,D00030004:1.8,ZC湛江市:1.8,773b1bb92295d931:0.0,K动漫大全:1.8,LC12:1.8,K年龄段:1.8\"}";
        TransportClient client = ESUtils.getClient();
        //ESUtils.putData(client,json,IndexName,"tag");
        //System.out.println(client.toString());
        ESUtils.createIndex(client,IndexName);
        //ESUtils.cleanUp(client);
    }


}
