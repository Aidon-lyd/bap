package dmp.util.redis;

import org.junit.After;
import org.junit.Before;
import redis.clients.jedis.Jedis;

/**
 * 使用Jedis客户端操作redis集群
 */
public class JedisClient {
    private Jedis jedis;

    /**
     * 共同的初始化操作
     */
    @Before
    public void setup() {jedis = new Jedis("node245", 6379, 2000); }

    //通过Redis数据库获取商圈
    public static String redisQuery(String geoHash){
        Jedis jedis = JedisPoolUtil.getJedisFromPool();
        String bussiness = jedis.get(geoHash);
        jedis.close();
        return bussiness;
    }

    //插入redis数据库
    public static void redisInsertByBussiness(String geoHash, String bussiness){
        Jedis jedis = JedisPoolUtil.getJedisFromPool();
        jedis.set(geoHash, bussiness);
        jedis.close();
    }

    /**
     * 释放资源
     */
    @After
    public void cleanUp(){
        if(jedis != null)
            jedis.close();
    }
}
