package dmp.util.redis;

import java.io.IOException;
import java.util.Properties;

/**
 * 资源文件操作工具类
 */
public class PropertiesUtil {
    private static Properties properties;

    static{
        properties = new Properties();

        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("jedis.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据key获得value
     */
    public static String getValueByKey(String key){
        return properties.getProperty(key);
    }

    /**
     * 根据key获得Int类型的value
     */
    public static int getIntValueByKey(String key){
        return Integer.parseInt(properties.getProperty(key));
    }

    /**
     * 根据key获得boolean类型的value
     */
    public static boolean getBooleanValueByKey(String key){
        return Boolean.valueOf(properties.getProperty(key));
    }
}
