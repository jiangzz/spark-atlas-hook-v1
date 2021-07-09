package com.jd.atlas.client.ext;

import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.model.notification.AtlasNotificationMessage;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.AbstractMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entity notification message deserializer.
 */
public class UserDefineEntityMessageDeserializer extends UserDefineAbstractMessageDeserializer<EntityNotification> {

    /**
     * Logger for entity notification messages.
     */
    private static final Logger NOTIFICATION_LOGGER = LoggerFactory.getLogger(org.apache.atlas.notification.entity.EntityMessageDeserializer.class);


    // ----- Constructors ----------------------------------------------------

    /**
     * Create an entity notification message deserializer.
     */
    public UserDefineEntityMessageDeserializer() {
        super(new TypeReference<EntityNotification>() {},
                new TypeReference<AtlasNotificationMessage<EntityNotification>>() {},
                AbstractNotification.CURRENT_MESSAGE_VERSION, NOTIFICATION_LOGGER);
    }

    @Override
    public EntityNotification deserialize(String messageJson) {
        final EntityNotification ret = super.deserialize(messageJson);

        if (ret != null) {
            ret.normalize();
        }

        return ret;
    }
}
