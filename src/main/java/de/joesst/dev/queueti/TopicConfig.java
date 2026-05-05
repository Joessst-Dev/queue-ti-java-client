package de.joesst.dev.queueti;

/**
 * Configuration settings for a single queue-ti topic.
 *
 * <p>All optional fields are represented as boxed types; {@code null} means "not set" and the
 * server will apply its own default. This mirrors Go's {@code omitempty} semantics.
 *
 * @param topic                the topic name; never {@code null}
 * @param maxRetries           maximum delivery attempts before a message is dead-lettered, or
 *                             {@code null} to use the server default
 * @param messageTtlSeconds    time-to-live for undelivered messages in seconds, or {@code null}
 * @param maxDepth             maximum number of messages the topic queue may hold, or {@code null}
 * @param replayable           whether the topic supports message replay
 * @param replayWindowSeconds  replay window duration in seconds, or {@code null}
 * @param throughputLimit      maximum messages per second, or {@code null}
 */
public record TopicConfig(
        String topic,
        Integer maxRetries,
        Integer messageTtlSeconds,
        Integer maxDepth,
        boolean replayable,
        Integer replayWindowSeconds,
        Integer throughputLimit) {
}
