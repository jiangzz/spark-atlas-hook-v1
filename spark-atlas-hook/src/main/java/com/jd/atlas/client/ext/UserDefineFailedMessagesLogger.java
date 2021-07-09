package com.jd.atlas.client.ext;

import org.apache.atlas.hook.FailedMessagesLogger;
import org.apache.log4j.*;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

public class UserDefineFailedMessagesLogger extends FailedMessagesLogger {
    public static final String PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE = "%d{ISO8601} %m%n";
    public static final String DATE_PATTERN = ".yyyy-MM-dd";

    private final Logger logger = Logger.getLogger(UserDefineFailedMessagesLogger.class);
    private String failedMessageFile;

    public UserDefineFailedMessagesLogger(String failedMessageFile) {
        super(failedMessageFile);
        this.failedMessageFile = failedMessageFile;
    }

    void init() {
        String rootLoggerDirectory = getRootLoggerDirectory();
        if (rootLoggerDirectory == null) {
            return;
        }
        String absolutePath = new File(rootLoggerDirectory, failedMessageFile).getAbsolutePath();
        try {
            DailyRollingFileAppender failedLogFilesAppender = new DailyRollingFileAppender(
                    new PatternLayout(PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE), absolutePath, DATE_PATTERN);
            logger.addAppender(failedLogFilesAppender);
            logger.setLevel(Level.ERROR);
            logger.setAdditivity(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the root logger file location under which the failed log messages will be written.
     *
     * Since this class is used in Hooks which run within JVMs of other components like Hive,
     * we want to write the failed messages file under the same location as where logs from
     * the host component are saved. This method attempts to get such a location from the
     * root logger's appenders. It will work only if at least one of the appenders is a {@link FileAppender}
     *
     * @return directory under which host component's logs are stored.
     */
    private String getRootLoggerDirectory() {
        String      rootLoggerDirectory = null;
        Logger      rootLogger          = Logger.getRootLogger();
        Enumeration allAppenders        = rootLogger.getAllAppenders();

        if (allAppenders != null) {
            while (allAppenders.hasMoreElements()) {
                Appender appender = (Appender) allAppenders.nextElement();

                if (appender instanceof FileAppender) {
                    FileAppender fileAppender   = (FileAppender) appender;
                    String       rootLoggerFile = fileAppender.getFile();

                    rootLoggerDirectory = rootLoggerFile != null ? new File(rootLoggerFile).getParent() : null;
                    break;
                }
            }
        }
        return rootLoggerDirectory;
    }

   public void log(String message) {
        logger.error(message);
    }
}
