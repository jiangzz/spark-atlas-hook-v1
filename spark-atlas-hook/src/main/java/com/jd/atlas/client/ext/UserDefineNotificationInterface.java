package com.jd.atlas.client.ext;

import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;

import java.util.List;

/**
 * Interface to the Atlas notification framework.
 * <p>
 * Use this interface to create consumers and to send messages of a given notification type.
 * <ol>
 *   <li>Atlas sends entity notifications
 *   <li>Hooks send notifications to create/update types/entities. Atlas reads these messages
 * </ol>
 */
public interface UserDefineNotificationInterface {
    /**
     * Prefix for Atlas notification related configuration properties.
     */
    String PROPERTY_PREFIX = "atlas.notification";

    /**
     * Atlas notification types.
     */
    enum NotificationType {
        // Notifications from the Atlas integration hooks.
        HOOK(new UserDefineHookMessageDeserializer()),

        // Notifications to entity change consumers.
        ENTITIES(new UserDefineEntityMessageDeserializer());

        private final UserDefineAtlasNotificationMessageDeserializer deserializer;

        NotificationType(UserDefineAtlasNotificationMessageDeserializer deserializer) {
            this.deserializer = deserializer;
        }

        public UserDefineAtlasNotificationMessageDeserializer getDeserializer() {
            return deserializer;
        }
    }

    /**
     *
     * @param user Name of the user under which the processes is running
     */
    void setCurrentUser(String user);

    /**
     * Create notification consumers for the given notification type.
     *
     * @param notificationType  the notification type (i.e. HOOK, ENTITIES)
     * @param numConsumers      the number of consumers to create
     * @param <T>               the type of the notifications
     *
     * @return the list of created consumers
     */
    <T> List<NotificationConsumer<T>> createConsumers(UserDefineNotificationInterface.NotificationType notificationType, int numConsumers);

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    <T> void send(UserDefineNotificationInterface.NotificationType type, T... messages) throws NotificationException;

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the list of messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    <T> void send(UserDefineNotificationInterface.NotificationType type, List<T> messages) throws NotificationException;

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
