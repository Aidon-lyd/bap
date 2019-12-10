package Spark_Utils

import java.util.concurrent.locks.{Lock, ReentrantLock}

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/10
  * Time: 8:48
  *
  * @author zbs
  * @version 1.0
  */
object Spark_Redis_Util {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var pool: JedisPool = null
  var lock: Lock = new ReentrantLock()
  val load = ConfigFactory.load("Redis.properties")
  val REDIS_SETMAXIDIE = "spark.redis.setMaxIdle"
  val REDIS_SETMAXTOTAL = "spark.redis.setMaxTotal"
  val REDIS_SETTESTONBORROW = "spark.redis.setTestOnBorrow"
  val REDIS_HOST = "spark.redis.host"
  val REDIS_PORT = "spark.redis.port"

  /**
    * @Author zbs
    * @Description:获得redis连接的方法
    * @Date: 2019/12/10
    * @return: void
    */
  def getConnect: Jedis = {
    if (pool == null) {
      lock.lock()
      try {
        if (pool == null) {
          val config = new JedisPoolConfig()
          config.setTestOnBorrow(load.getBoolean(REDIS_SETTESTONBORROW))
          config.setMaxIdle(load.getInt(REDIS_SETMAXIDIE))
          config.setMaxTotal(load.getInt(REDIS_SETMAXTOTAL))
          pool = new JedisPool(config, load.getString(REDIS_HOST), load.getInt(REDIS_PORT))
        }
        //        pool.getResource
      }
      catch {
        case ex: Exception => {
          ex.printStackTrace()
          logger.error(s"创建redis连接时,error${ex.getStackTrace}")
        }
      }
      finally {
        lock.unlock()
        //        pool.getResource
      }

    }
    pool.getResource
  }


  /**
    * @Author zbs
    * @Description:资源回收
    * @Date: 2019/12/10
    * @Param jedis:
    * @return: void
    */
  def releaseReource(jedis: Jedis) = {
    if (jedis != null) {
      pool.returnResource(jedis)
    }
  }


}
