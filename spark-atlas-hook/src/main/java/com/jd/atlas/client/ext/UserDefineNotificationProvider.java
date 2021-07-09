package com.jd.atlas.client.ext;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class UserDefineNotificationProvider {
    public static final Logger log= LoggerFactory.getLogger(UserDefineNotificationProvider.class);

    private static UserDefineKafkaNotification kafka;
    public static  UserDefineKafkaNotification get() {
        if (kafka == null) {
            try {
                kafka = new UserDefineKafkaNotification(AtlasProperties.properties);
            } catch (Exception e) {
                log.error("错误加载{}文件！","spark-hook.properties");
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }
}
