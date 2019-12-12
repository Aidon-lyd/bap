package com.cmcc.dmp.Utils;

import com.cmcc.dmp.common.CommonData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * requirement
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/10 8:58
 */
public class JedisPoolUtils {

    private static JedisPool jedisPool = null;

    private JedisPoolUtils() {

    }

    public static Jedis getJedisPool() {

        if (jedisPool == null){
            synchronized (JedisPoolUtils.class){
                if (jedisPool == null){
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxIdle(PropertiesJedisPoolUtils.getIntValueByKey(CommonData.JEDIS_MAX_IDLE));
                    poolConfig.setMaxTotal(PropertiesJedisPoolUtils.getIntValueByKey(CommonData.JEDIS_MAX_TOTAL));
                    poolConfig.setTestOnBorrow(PropertiesJedisPoolUtils.getBooleanValueByKey(CommonData.JEDIS_ON_BORROW));

                    jedisPool = new JedisPool(poolConfig,
                            PropertiesJedisPoolUtils.getStringValueByKey(CommonData.JEDIS_HOST),
                            PropertiesJedisPoolUtils.getIntValueByKey(CommonData.JEDIS_PORT));
                }
            }
        }
        return jedisPool.getResource();
    }

    /**
     * 释放资源，将连接返回池子
     * @param jedis
     */
    public static void releaseResource(Jedis jedis){
        if (jedis!=null){
            jedisPool.returnResourceObject(jedis);
        }
    }
}
