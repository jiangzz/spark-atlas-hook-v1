package com.jd.atlas.client.ext;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.model.notification.AtlasNotificationMessage;
import org.apache.atlas.model.notification.MessageVersion;
import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;
import org.slf4j.Logger;

/**
 * Base notification message deserializer.
 */
public abstract class UserDefineAbstractMessageDeserializer<T> extends UserDefineAtlasNotificationMessageDeserializer<T> {

    // ----- Constructors ----------------------------------------------------

    /**
     * Create a deserializer.
     *
     * @param expectedVersion         the expected message version
     * @param notificationLogger      logger for message version mismatch
     */
    public UserDefineAbstractMessageDeserializer(TypeReference<T> messageType,
                                                 TypeReference<AtlasNotificationMessage<T>> notificationMessageType,
                                                 MessageVersion expectedVersion, Logger notificationLogger) {
        super(messageType, notificationMessageType, expectedVersion, notificationLogger);
    }


    // ----- helper methods --------------------------------------------------
}
