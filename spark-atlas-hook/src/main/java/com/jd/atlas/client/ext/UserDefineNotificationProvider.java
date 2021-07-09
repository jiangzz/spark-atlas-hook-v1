package com.jd.atlas.client.ext;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class UserDefineNotificationProvider {
    private static UserDefineKafkaNotification kafka;


    public static  UserDefineKafkaNotification get() {
        if (kafka == null) {
            try {
                InputStream inputStream = UserDefineNotificationProvider.class.getClassLoader().getResourceAsStream("atlas-application.properties");
                Properties atlasProperties = new Properties();
                atlasProperties.load(inputStream);
                kafka = new UserDefineKafkaNotification(atlasProperties);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }
}
