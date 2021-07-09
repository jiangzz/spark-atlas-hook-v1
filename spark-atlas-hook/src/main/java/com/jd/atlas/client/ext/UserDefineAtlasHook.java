package com.jd.atlas.client.ext;
import org.apache.atlas.hook.AtlasHook;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;


/**
 * A base class for atlas hooks.
 */
public abstract class UserDefineAtlasHook extends AtlasHook {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHook.class);

    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS                    = "atlas.notification.hook.asynchronous";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_MIN_THREADS        = "atlas.notification.hook.asynchronous.minThreads";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_MAX_THREADS        = "atlas.notification.hook.asynchronous.maxThreads";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS = "atlas.notification.hook.asynchronous.keepAliveTimeMs";
    public static final String ATLAS_NOTIFICATION_MAX_RETRIES                     = "atlas.notification.hook.retry.maxRetries";
    public static final String ATLAS_NOTIFICATION_RETRY_INTERVAL                  = "atlas.notification.hook.retry.interval";
    public static final String ATLAS_NOTIFICATION_FAILED_MESSAGES_FILENAME_KEY    = "atlas.notification.failed.messages.filename";
    public static final String ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY = "atlas.notification.log.failed.messages";
    public static final String ATLAS_HOOK_FAILED_MESSAGES_LOG_DEFAULT_NAME        = "atlas_hook_failed_messages.log";
    public static final String CONF_METADATA_NAMESPACE                            = "atlas.metadata.namespace";
    public static final String CLUSTER_NAME_KEY                                   = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME                               = "primary";

    protected static Properties atlasProperties;
    protected static NotificationInterface notificationInterface;

    private static final String               metadataNamespace;
    private static final int                  SHUTDOWN_HOOK_WAIT_TIME_MS = 3000;
    private static final boolean              logFailedMessages;
    private static final UserDefineFailedMessagesLogger failedMessagesLogger;
    private static final int                  notificationMaxRetries;
    private static final int                  notificationRetryInterval;
    private static       ExecutorService      executor = null;


    static {
        try {
            InputStream inputStream = UserDefineNotificationProvider.class.getClassLoader().getResourceAsStream("atlas-application.properties");
            atlasProperties = new Properties();
            atlasProperties.load(inputStream); //ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        String failedMessageFile = atlasProperties.getProperty(ATLAS_NOTIFICATION_FAILED_MESSAGES_FILENAME_KEY, ATLAS_HOOK_FAILED_MESSAGES_LOG_DEFAULT_NAME);

        logFailedMessages = Boolean.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY, "true"));

        if (logFailedMessages) {
            failedMessagesLogger = new UserDefineFailedMessagesLogger(failedMessageFile);
            failedMessagesLogger.init();
        } else {
            failedMessagesLogger = null;
        }

        metadataNamespace         = getMetadataNamespace(atlasProperties);
        notificationMaxRetries    = Integer.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_MAX_RETRIES, "3"));
        notificationRetryInterval = Integer.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_RETRY_INTERVAL, "1000"));
        //用户自定义UserDefineNotificationProvider
        notificationInterface     = UserDefineNotificationProvider.get();

        String currentUser = "";

        try {
            currentUser = getUser();
        } catch (Exception excp) {
            LOG.warn("Error in determining current user", excp);
        }

        notificationInterface.setCurrentUser(currentUser);

        boolean isAsync = Boolean.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_ASYNCHRONOUS, "true"));

        if (isAsync) {
            int  minThreads      = Integer.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_ASYNCHRONOUS_MIN_THREADS, "1"));
            int  maxThreads      = Integer.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_ASYNCHRONOUS_MAX_THREADS, "1"));
            long keepAliveTimeMs = Long.valueOf(atlasProperties.getProperty(ATLAS_NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS, "10000"));

            executor = new ThreadPoolExecutor(minThreads,
                    maxThreads,
                    keepAliveTimeMs,
                    TimeUnit.MILLISECONDS,
                    new SynchronousQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat("Atlas Notifier %d").setDaemon(true).build());

            ShutdownHookManager.get().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        LOG.info("==> Shutdown of Atlas Hook");
                        executor.shutdown();
                        executor.awaitTermination(SHUTDOWN_HOOK_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                        executor = null;
                    } catch (InterruptedException excp) {
                        LOG.info("Interrupt received in shutdown.", excp);
                    } finally {
                        LOG.info("<== Shutdown of Atlas Hook");
                    }
                }
            }, AtlasConstants.ATLAS_SHUTDOWN_HOOK_PRIORITY);
        }

        LOG.info("Created Atlas Hook");
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages   hook notification messages
     * @param maxRetries maximum number of retries while sending message to messaging system
     */
    public static void notifyEntities(final List<HookNotification> messages, final UserGroupInformation ugi, final  int maxRetries) {
        final String messagaKey = NotificationContextHolder.getMessagaKey();

        if (executor == null) { // send synchronously
            notifyEntitiesInternal(messages, maxRetries, ugi, notificationInterface, logFailedMessages, failedMessagesLogger);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    NotificationContextHolder.setMessageKey(messagaKey);
                    notifyEntitiesInternal(messages, maxRetries, ugi, notificationInterface, logFailedMessages, failedMessagesLogger);
                    NotificationContextHolder.clear();
                }
            });
        }
    }

    static void notifyEntitiesInternal(final List<HookNotification> messages, int maxRetries, UserGroupInformation ugi,
                                       final NotificationInterface notificationInterface,
                                       boolean shouldLogFailedMessages, UserDefineFailedMessagesLogger logger) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        final int maxAttempts         = maxRetries < 1 ? 1 : maxRetries;
        Exception notificationFailure = null;

        for (int numAttempt = 1; numAttempt <= maxAttempts; numAttempt++) {
            if (numAttempt > 1) { // retry attempt
                try {
                    LOG.debug("Sleeping for {} ms before retry", notificationRetryInterval);

                    Thread.sleep(notificationRetryInterval);
                } catch (InterruptedException ie) {
                    LOG.error("Notification hook thread sleep interrupted");

                    break;
                }
            }

            try {
                if (ugi == null) {
                    notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                } else {
                    PrivilegedExceptionAction<Object> privilegedNotify = new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                            return messages;
                        }
                    };

                    ugi.doAs(privilegedNotify);
                }

                notificationFailure = null; // notification sent successfully, reset error

                break;
            } catch (Exception e) {
                notificationFailure = e;

                LOG.error("Failed to send notification - attempt #{}; error={}", numAttempt, e.getMessage());
            }
        }

        if (notificationFailure != null) {
            if (shouldLogFailedMessages && notificationFailure instanceof NotificationException) {
                final List<String> failedMessages = ((NotificationException) notificationFailure).getFailedMessages();

                for (String msg : failedMessages) {
                    logger.log(msg);
                }
            }

            LOG.error("Giving up after {} failed attempts to send notification to Atlas: {}", maxAttempts, messages.toString(), notificationFailure);
        }
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages hook notification messages
     */
    @Override
    protected void notifyEntities(List<HookNotification> messages, UserGroupInformation ugi) {
        notifyEntities(messages, ugi, notificationMaxRetries);
    }

    /**
     * Returns the logged in user.
     *
     * @return
     */
    public static String getUser() {
        return getUser(null, null);
    }

    public static String getUser(String userName) {
        return getUser(userName, null);
    }

    /**
     * Returns the user. Order of preference:
     * 1. Given userName
     * 2. ugi.getShortUserName()
     * 3. UserGroupInformation.getCurrentUser().getShortUserName()
     * 4. System.getProperty("user.name")
     */

    public static String getUser(String userName, UserGroupInformation ugi) {
        if (StringUtils.isNotEmpty(userName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning userName {}", userName);
            }
            return userName;
        }

        if (ugi != null && StringUtils.isNotEmpty(ugi.getShortUserName())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning ugi.getShortUserName {}", userName);
            }
            return ugi.getShortUserName();
        }

        try {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
            LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
            return System.getProperty("user.name");
        }
    }

    private static String getMetadataNamespace(Properties config) {
        return config.getProperty(CONF_METADATA_NAMESPACE, getClusterName(config));
    }

    private static String getClusterName(Properties config) {
        return config.getProperty(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    @Override
    public String getMetadataNamespace() {
        return metadataNamespace;
    }
}