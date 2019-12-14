package dmp.util.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Jedis池工具类，使用资源文件工具类，共同类进行代码优化
 */
public class JedisPoolUtil {
    private static JedisPool pool;

    /**
     * 获得Jedis的实例
     */
    public static Jedis getJedisFromPool(){
        if (pool == null) {//外层的if是为了提高效率
            synchronized (JedisPoolUtil.class){
                if (pool == null) { //该if是为了保证JedisPool实例的唯一性（单例）
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(PropertiesUtil.getIntValueByKey(CommonData.JEDIS_MAX_IDLE));
                    config.setMaxTotal(PropertiesUtil.getIntValueByKey(CommonData.JEDIS_MAX_TOTAL));
                    config.setTestOnBorrow(PropertiesUtil.getBooleanValueByKey(CommonData.JEDIS_ON_BORROW));
                    pool = new JedisPool(config, PropertiesUtil.getValueByKey(CommonData.JEDIS_HOST),
                            PropertiesUtil.getIntValueByKey(CommonData.JEDIS_PORT));
                }
            }
        }
        return pool.getResource();
    }

    /**
     * 将当前的Jedis实例归还到Jedis池中存储起来
     */
    public static void releaseResource(Jedis jedis){
        if(jedis != null){
            pool.returnResourceObject(jedis);
        }
    }
}
