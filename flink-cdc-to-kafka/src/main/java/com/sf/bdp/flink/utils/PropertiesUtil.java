package com.sf.bdp.flink.utils;

import com.sf.bdp.flink.ApplicationParameter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.Properties;

public class PropertiesUtil {

    /**
     * 利用反射获取给对象设置值
     *
     * @param object
     * @param fileName
     * @param <T>
     * @return
     */
    public static <T> T loadObjectByProperties(T object, String fileName) {
        try {
            Properties properties = loadProperties(fileName);
            Class clazz = object.getClass();
            for (String name : properties.stringPropertyNames()) {
                String value = properties.getProperty(name);
                String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
                Method method = clazz.getMethod(methodName, String.class);
                method.invoke(object, value);
            }
            return object;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Properties loadProperties(String fileName) {
        try {
            Properties properties = new Properties();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            properties.load(bufferedReader);
            return properties;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        ApplicationParameter param = new ApplicationParameter();
        ApplicationParameter applicationParameter = PropertiesUtil.loadObjectByProperties(param, System.getProperty("user.dir") + "\\conf\\mysql2kafka.properties");
        System.out.println(applicationParameter);
    }

}
