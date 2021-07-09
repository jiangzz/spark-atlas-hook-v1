package com.jd.atlas.client.ext;

import org.apache.spark.internal.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class AtlasProperties  {
    public static final Logger log= LoggerFactory.getLogger(AtlasProperties.class);
    public static final Properties properties;
    static {
        try {
            log.info("开始加载{}文件！","spark-hook.properties");
            InputStream inputStream = AtlasProperties.class.getClassLoader().getResourceAsStream("spark-hook.properties");
            properties = new Properties();
            properties.load(inputStream);
            log.info("==============初始化参数=======================");
            for (Map.Entry<Object, Object> objectObjectEntry : properties.entrySet()) {
                log.info("{}\t{}",objectObjectEntry.getKey(),objectObjectEntry.getValue());
            }

        } catch (IOException e) {
            log.error("没有加载{}文件!","spark-hook.properties");
            throw new RuntimeException("没有找到:spark-hook.properties文件");
        }
    }
    public static String getMetaNamespace(){
        return properties.getProperty("atlas.metadata.namespace","default");
    }
    public static String getHook(){
        return properties.getProperty("atlas.notification.hook.topic.name","ATLAS_HOOK");
    }
}
