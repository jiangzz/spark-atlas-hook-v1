package com.jd.atlas.client.ext;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.model.notification.AtlasNotificationMessage;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.AbstractMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Hook notification message deserializer.
 */
public class UserDefineHookMessageDeserializer extends UserDefineAbstractMessageDeserializer<HookNotification> {

    /**
     * Logger for hook notification messages.
     */
    private static final Logger NOTIFICATION_LOGGER = LoggerFactory.getLogger(UserDefineHookMessageDeserializer.class);


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a hook notification message deserializer.
     */
    public UserDefineHookMessageDeserializer() {
        super(new TypeReference<HookNotification>() {},
                new TypeReference<AtlasNotificationMessage<HookNotification>>() {},
                AbstractNotification.CURRENT_MESSAGE_VERSION, NOTIFICATION_LOGGER);
    }

    @Override
    public HookNotification deserialize(String messageJson) {
        final HookNotification ret = super.deserialize(messageJson);

        if (ret != null) {
            ret.normalize();
        }

        return ret;
    }
}