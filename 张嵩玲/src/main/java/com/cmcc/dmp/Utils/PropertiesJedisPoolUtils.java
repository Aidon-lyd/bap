package com.cmcc.dmp.Utils;

import com.cmcc.dmp.common.CommonData;

import java.io.IOException;
import java.util.Properties;

/**
 * requirement
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/10 9:14
 */
public class PropertiesJedisPoolUtils {
    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(PropertiesJedisPoolUtils.class.getClassLoader()
            .getResourceAsStream(CommonData.PROPERTIES_FILE));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 根据key获得String类型的value
     * @param key
     * @return
     */
    public static String getStringValueByKey(String key){
        return properties.getProperty(key);
    }

    /**
     * 根据key获得String类型的value
     * @param key
     * @return
     */
    public static Integer getIntValueByKey(String key){
        return Integer.parseInt(properties.getProperty(key));
    }

    /**
     * 根据key获得String类型的value
     * @param key
     * @return
     */
    public static Boolean getBooleanValueByKey(String key){
        return Boolean.valueOf(properties.getProperty(key));
    }

}
