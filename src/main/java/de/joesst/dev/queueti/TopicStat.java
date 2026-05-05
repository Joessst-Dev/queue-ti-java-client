package de.joesst.dev.queueti;

/**
 * Runtime statistics for a single queue-ti topic.
 *
 * @param topic  the topic name; never {@code null}
 * @param status a human-readable status string (e.g. {@code "active"}); never {@code null}
 * @param count  the number of messages currently queued
 */
public record TopicStat(
        String topic,
        String status,
        int count) {
}
