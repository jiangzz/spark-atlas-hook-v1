package com.jd.atlas.client.ext;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URL;

public class UserDefineNotificationProvider {
    private static UserDefineKafkaNotification kafka;


    public static  UserDefineKafkaNotification get() {
        if (kafka == null) {
            try {
                URL url = UserDefineNotificationProvider.class.getClassLoader().getResource("atlas-application.properties");
                System.out.println("URL:"+url);
                Configuration applicationProperties = new PropertiesConfiguration(url) ;
                kafka = new UserDefineKafkaNotification(applicationProperties);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }
}
