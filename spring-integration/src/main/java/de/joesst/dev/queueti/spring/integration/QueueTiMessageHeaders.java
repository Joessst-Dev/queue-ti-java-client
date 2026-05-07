package de.joesst.dev.queueti.spring.integration;

/**
 * Spring Integration message header name constants for queue-ti messages.
 */
public final class QueueTiMessageHeaders {

    public static final String MESSAGE_ID     = "queueti_messageId";
    public static final String TOPIC          = "queueti_topic";
    public static final String RETRY_COUNT    = "queueti_retryCount";
    public static final String CREATED_AT     = "queueti_createdAt";
    public static final String METADATA       = "queueti_metadata";
    /** Present only in {@code MANUAL} acknowledge mode. */
    public static final String ACKNOWLEDGMENT = "queueti_acknowledgment";

    private QueueTiMessageHeaders() {}
}
